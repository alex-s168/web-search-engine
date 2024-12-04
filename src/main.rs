#![feature(backtrace_frames)]

use texting_robots::{Robot, get_robots_url};
use std::sync::Arc;
use kuchiki::traits::TendrilSink;
use charabia::Tokenize;
use std::collections::{HashMap, HashSet};
use regex::Regex;
use clickhouse::{Client, Row};
use serde::{Serialize, Deserialize};
use anyhow::{Error, Result};
use std::time::Duration;
use std::iter::Iterator;
use concurrent_queue::ConcurrentQueue;

struct IntoChunks<T, B: Iterator<Item = T>> {
    num: usize,
    backing: B
}

impl<T, B: Iterator<Item = T>> Iterator for IntoChunks<T,B> {
    type Item = Vec<T>;
    fn next(&mut self) -> Option<Vec<T>> {
        let mut out = Vec::new();
        while out.len() < self.num {
            if let Some(x) = self.backing.next() {
                out.push(x);
            } else { break; }
        }
        if out.len() == 0 { None } else { Some(out) }
    }
}

fn into_chunks<T, B: Iterator<Item = T>>(iter: B, num: usize) -> IntoChunks<T, B> {
    IntoChunks { num, backing: iter }
}

#[derive(Row, Serialize, Deserialize, Clone)]
struct DbScraped {
    url: String,
    #[serde(with = "clickhouse::serde::time::datetime")]
    time: time::OffsetDateTime,
    title: Option<String>,
}

#[derive(Row, Serialize, Deserialize)]
struct DbWord {
    word: String,
    url: String,
    num: u64
}

#[derive(Row, Serialize, Deserialize, Clone)]
struct DbToScrap {
    url: String,
}

#[derive(Row, Serialize, Deserialize, Debug)]
struct DbLink {
    from_url: String,
    to_url: String,
}

async fn scrape(vec: &Vec<u8>) -> Result<(HashMap<String, usize>, Vec<String>, Option<String>)> {
    let str = std::str::from_utf8(vec.as_slice())?;

    let parser = kuchiki::parse_html().one(str);

    parser
        .inclusive_descendants()
        .filter(|node| {
            node.as_element().map_or(false, |e| {
                matches!(e.name.local.as_ref(), "script" | "style" | "noscript")
            })
        })
        .collect::<Vec<_>>()
        .iter()
        .for_each(|node| node.detach());

    let mut title = None;

    parser 
        .inclusive_descendants()
        .filter(|node| {
            node.as_element().map_or(false, |e| {
                matches!(e.name.local.as_ref(), "title")
            })
        })
        .for_each(|node| {
            if title.is_none() {
                if let Some(x) = node.as_text() {
                    title = Some(x.borrow().clone());
                }
            }
        });

    let urls = parser 
        .inclusive_descendants()
        .filter(|node| {
            node.as_element().map_or(false, |e| {
                matches!(e.name.local.as_ref(), "a")
            })
        })
        .filter_map(|node| {
            node.as_element()
                .and_then(|x| x
                    .attributes
                    .borrow()
                    .get("href")
                    .map(|x| x.to_string()))
        })
        .collect();

    let words_re = Regex::new(r"^(?:[a-zA-Z]+[_\-\s]*)?(?:[a-zA-Z0-9]+[_\-\s]*)*$")?;
    let mut dict = HashMap::<String, usize>::new();
    parser.text_contents()
        .as_str()
        .tokenize()
        .for_each(|tok| {
            let s = tok.lemma();
            if words_re.is_match(s) {
                if let Some(x) = dict.get_mut(s) {
                    *x += 1;
                } else {
                    dict.insert(s.to_string(), 1);
                }
            }
        });

    Ok((dict, urls, title))
}

async fn get_robots(url: &str) -> Option<Robot> {
    let url = get_robots_url(url).ok()?;
    let body = reqwest::get(url).await.ok()?.bytes().await.ok()?;
    Robot::new("AlexWebBot", &body).ok()
}

#[derive(Serialize, Deserialize)]
struct Config {
    db_url: String,
    db_user: Option<String>,
    db_password: Option<String>,
    db_headers: Option<HashMap<String, String>>,
    db_name: String,
    https_proxy: String,
    num_threads: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            db_url: "http://localhost:8123".to_string(),
            db_user: None,
            db_password: None,
            db_headers: None,
            db_name: "pages".to_string(),
            https_proxy: Self::INVALID_PROXY.to_string(),
            num_threads: num_cpus::get() * 6
        }
    }
}

impl Config {
    const INVALID_PROXY: &'static str = "INSERT_PROXY_HERE";

    fn is_valid(&self) -> bool {
        self.https_proxy != Self::INVALID_PROXY
    }

    fn read(path: &std::path::Path) -> Result<Config> {
        if let Ok(body) = std::fs::read_to_string(path) {
            let config = serde_json::from_str::<Config>(body.as_str())?;
            if config.is_valid() {
                Ok(config)
            } else {
                Err(Error::msg("you need to set the proxy!"))
            }
        } else {
            std::fs::write(path, &serde_json::to_string_pretty(&Config::default())?)?;
            Err(Error::msg("you need to set the proxy!"))
        }
    }
}

struct ScrapeShared {
    config: Config,
    db: Client,
    scraped: ConcurrentQueue<DbScraped>, // "scraped"
    todo: ConcurrentQueue<String>,
    links: ConcurrentQueue<DbLink>,
}

async fn insert_todo(db: &Client, to_scrape: &[String]) -> Result<()> {
    db.query("INSERT INTO to_query
              SELECT value
              FROM (SELECT arrayJoin(?) AS value)
              WHERE NOT value IN (SELECT url FROM scraped)")
        .bind(to_scrape)
        .execute()
        .await?;
    Ok(())
}

async fn do_scrape(shared: Arc<ScrapeShared>, website: String) -> Result<()> {
    let website = website.as_str();

    let bad_url_regex = Regex::new(r"(?i)^.*\.(jpg|jpeg|png|zip|exe|webp|svg|css|js|pdf)([#\?].*)?$")?;

    if bad_url_regex.is_match(website) {
        return Ok(());
    }

    let url_regex = [
        Regex::new(r"https?:\/\/(.*?)(#.*|$)")?,
        Regex::new(r"//(.*?)(#.*|$)")?,
    ];

    let robot = get_robots(website).await.unwrap_or(Robot::new("AlexWebBot", b"").unwrap());
    if !robot.allowed(website) {
        return Ok(());
    }

    let proxy = reqwest::Proxy::https(shared.config.https_proxy.as_str())?;
    let vec = reqwest::Client::builder()
        .proxy(proxy)
        .build()?
        .get(website)
        .timeout(Duration::from_secs(6))
        .send()
        .await;
    
    let vec = if let Ok(vec) = vec {
        if let Ok(vec) = vec.bytes().await {
            vec.to_vec()
        } else {
            println!("skipping {} because invalid response", website);
            return Ok(());
        }
    } else {
        println!("skipping {} because not reachable", website);
        return Ok(());
    };

    let (words, urls, title) = scrape(&vec).await?;

    let scraped = DbScraped {
        url: website.to_string(),
        time: time::OffsetDateTime::now_utc(),
        title
    };

    if let Err(_) = shared.scraped.push(scraped.clone()) {
        let mut inserter = shared.db.inserter("scraped")?;
        inserter.write(&scraped)?;
        inserter.end().await?;
    }

    let mut words_inserter = shared.db.inserter("words")?;
    for word in words {
        words_inserter.write(&DbWord {
            word: word.0.clone(),
            num: word.1 as u64,
            url: website.to_string()
        })?;
    }
    words_inserter.end().await?;

    let mut to_scrape = HashSet::new();
    for url in urls {
        for re in &url_regex {
            if let Some(x) = re.captures(url.as_str()) {
                let addr = "http://".to_string() + x.get(1).unwrap().as_str();
                if !bad_url_regex.is_match(addr.as_str()) {
                    to_scrape.insert(addr);
                }
                break;
            }
        }
    }

    if to_scrape.len() > 0 {
        let to_scrape = to_scrape.into_iter().collect::<Vec<_>>();
        if to_scrape.len() >= 32 {
            insert_todo(&shared.db, to_scrape.as_slice()).await?;

            let mut links_inserter = shared.db.inserter("links")?;
            for x in to_scrape {
                links_inserter.write(&DbLink {
                    from_url: website.to_string(),
                    to_url: x.to_string()
                })?;
            }
            links_inserter.end().await?;
        } else {
            for x in to_scrape {
                shared.todo.push(x.clone())?;
                shared.links.push(DbLink {
                    from_url: website.to_string(),
                    to_url: x
                })?;
            }
        }
    }

    Ok(())
}

async fn next_batch_flush(db: &Client, table_name: &str, batch: &Vec<DbToScrap>) -> Result<()> {
    db.query(format!(" DELETE FROM {} WHERE `url` in ?", table_name).as_str())
        .bind(batch.iter().map(|x| x.url.clone()).collect::<Vec<_>>())
        .execute()
        .await?;

    Ok(())
}

async fn scrape_task(_permit: tokio::sync::OwnedSemaphorePermit, shared: Arc<ScrapeShared>, todo: DbToScrap) {
    let web = todo.url.clone();

    let res = do_scrape(shared.clone(), web.clone()).await;

    match res {
        Ok(()) => {}
        Err(err) => {
            let mut s = String::new();
            for x in err.backtrace().frames() {
                s += "\n  ";
                s += format!("{:?}", x).as_str();
            }
            eprintln!("while processing {}:\n  {}{}", web, err, s);
            if let Err(_) = shared.todo.push(web.clone()) {
                if let Ok(mut todo_inserter) = shared.db.inserter("to_query") {
                    let _ = todo_inserter.write(&DbToScrap { url: web });
                    let _ = todo_inserter.end().await;
                }
            }
        }
    }

    let _ = next_batch_flush(&shared.db, "to_query", &vec!(todo.clone())).await;
    let _ = next_batch_flush(&shared.db, "force_query", &vec!(todo)).await;
}

#[tokio::main]
async fn main() -> Result<()> {
    let cfg_path = std::env::current_dir()?.join("config.json");
    println!("reading config {}", cfg_path.as_path().display());
    let config = Config::read(cfg_path.as_path())?;
    let mut db = Client::default()
        .with_url(config.db_url.as_str())
        .with_option("async_insert", "1")
        .with_option("wait_for_async_insert", "0")
        .with_database(config.db_name.as_str());
    if let Some(x) = &config.db_user {
        db = db.with_user(x.as_str());
    }
    if let Some(x) = &config.db_password {
        db = db.with_password(x.as_str());
    }
    if let Some(x) = &config.db_headers {
        for (k,v) in x {
            db = db.with_header(k.as_str(), v.as_str());
        }
    }
    let num_threads = config.num_threads;
    let shared = Arc::new(ScrapeShared {
        config,
        db,
        scraped: ConcurrentQueue::bounded(20 * num_threads),
        todo: ConcurrentQueue::bounded(100 * num_threads),
        links: ConcurrentQueue::bounded(100 * num_threads),
    });

    let proc_thread = tokio::task::spawn({
        let shared = shared.clone();
        async move {
            let mut interval = tokio::time::interval(Duration::from_secs(4));

            loop {
                interval.tick().await;

                let mut data = Vec::new();
                for todo in shared.todo.try_iter() {
                    data.push(todo);
                }
                if data.len() > 0 {
                    insert_todo(&shared.db, data.as_slice()).await.unwrap();
                }

                {
                    let mut inserter = shared.db.inserter("scraped").unwrap();
                    for sc in shared.scraped.try_iter() {
                        inserter.write(&sc).unwrap();
                    }
                    inserter.end().await.unwrap();
                }

                {
                    let mut inserter = shared.db.inserter("links").unwrap();
                    for li in shared.links.try_iter() {
                        inserter.write(&li).unwrap();
                    }
                    inserter.end().await.unwrap();
                }
            }
        }
    });

    let threads_sem = Arc::new(tokio::sync::Semaphore::new(shared.config.num_threads));

    let mut todo_queue = Vec::<DbToScrap>::new();

    loop {
        if proc_thread.is_finished() {
            eprintln!("db flush thread crashed. exiting");
            break;
        }

        if todo_queue.len() == 0 {
            println!("fetching chunk of todos");
            todo_queue = shared.db.query("SELECT * FROM (SELECT * FROM pages.force_query ORDER BY rand() UNION ALL SELECT * FROM pages.to_query ORDER BY rand()) LIMIT 128")
                .fetch_all::<DbToScrap>()
                .await?;
        }

        if todo_queue.len() == 0 {
            break;
        }

        let permit = threads_sem.clone().acquire_owned().await?;
        let url = todo_queue.pop().unwrap();
        println!("{}", url.url);
        tokio::spawn(scrape_task(permit, shared.clone(), url));
    }

    Ok(())
}
