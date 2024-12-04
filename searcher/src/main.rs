use clickhouse::{Client, Row};
use anyhow::Result;
use charabia::Tokenize;
use std::collections::HashMap;
use regex::Regex;
use serde::{Serialize, Deserialize};
#[macro_use] extern crate rocket;
use rocket::fs::{FileServer, relative};

#[derive(Row, Serialize, Deserialize)]
struct Res {
    url: String,
    score: f64
}

#[derive(Row, Serialize, Deserialize)]
struct DbToScrap {
    url: String,
}

#[derive(Row, Serialize, Deserialize, Debug)]
struct DbLink {
    from_url: String,
    to_url: String,
}

async fn autocomplete_word(db: &Client, word: &str, limit: usize) -> Result<Vec<String>> {
    let r = db.query(format!(r"SELECT word FROM pages.num_words WHERE startsWith(word, ?) ORDER BY total_num DESC LIMIT {}", limit).as_str())
        .bind(word)
        .fetch_all::<String>()
        .await?;
    Ok(r)
}

async fn autocomplete(db: &Client, input: &str, limit: usize) -> Result<Vec<String>> {
    let toks = input.tokenize()
        .collect::<Vec<_>>();

    if toks.len() == 0 {
        return Ok(vec!(input.to_string()));
    }

    let without_last = &input[0..toks.iter().last().unwrap().char_start];

    let res = autocomplete_word(db, toks.iter().last().unwrap().lemma(), limit).await?;

    Ok(res.into_iter().map(|x| {
        let mut res = without_last.to_string();
        res += x.as_str();
        res
    }).collect::<Vec<_>>())
}

async fn find_results(db: &Client, word: &str) -> Result<Vec<Res>> {
    let r = db.query(r"
                    SELECT
                        w.url,
                        CAST(((CAST(l.val AS Float64) + 1) *
                        (CAST(w.num AS Float64) / (SELECT total_num FROM pages.num_words FINAL WHERE word = ?))) AS Float64) AS score
                    FROM pages.words AS w
                    RIGHT JOIN (SELECT * FROM pages.num_links FINAL) AS l ON (l.url = w.url)
                    WHERE w.word = ?
                    GROUP BY
                        w.url, w.num, l.val
                    ORDER BY score DESC
                    LIMIT 50
                    SETTINGS join_algorithm='parallel_hash'
                    ")
        .bind(word)
        .bind(word)
        .fetch_all::<Res>()
        .await?;

    Ok(r)
}

fn lemma(search: &str) -> Vec<String> {
    let words_re = Regex::new(r"^(?:[a-zA-Z]+[_\-\s]*)?(?:[a-zA-Z0-9]+[_\-\s]*)*$").unwrap();
    search.tokenize()
        .map(|x| x.lemma().to_string())
        .filter(|x| words_re.is_match(x.as_str()))
        .collect::<Vec<_>>()
}

async fn run_search(db: &Client, search: &str) -> Result<Vec<(String, f64)>> {
    let toks = lemma(search);

    let mut scores = HashMap::<String, f64>::new();

    let results = toks.iter()
        .map(|w| find_results(&db, w.as_str()));

    for r in results {
        for Res { url, score } in r.await? {
            if let Some(num) = scores.get_mut(&url) {
                *num *= score + 0.001;
            } else {
                scores.insert(url, score + 0.001);
            }
        }
    }

    let mut web_scores = scores
        .into_iter()
        .filter(|(_, n)| *n >= 0.001)
        .collect::<Vec<_>>();

    web_scores.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap().reverse());
    Ok(web_scores)
}

fn open_db() -> Client {
    Client::default()
        .with_url("http://localhost:8123")
        .with_option("async_insert", "1")
        .with_option("wait_for_async_insert", "0")
        .with_database("pages")
}

#[get("/search/<what>")]
async fn search_endpoint(what: &str) -> String {
    let db = open_db();
    if let Ok(mut res) = run_search(&db, what).await {
        let mut s = String::new();
        if res.len() > 50 {
            res.drain(50..);
        }
        for (x, _) in res {
            s += x.as_str();
            s += "\n";
        }
        s
    } else {
        "error".to_string()
    }
}

#[get("/complete/<word>/<limit>")]
async fn complete_endpoint(word: &str, limit: usize) -> String {
    let db = open_db();
    if let Ok(res) = autocomplete(&db, word, limit).await {
        let mut s = String::new();
        for x in res {
            s += x.as_str();
            s += "\n";
        }
        s
    } else {
        "error".to_string()
    }
}

#[get("/tok/<what>")]
async fn lemma_endpoint(what: &str) -> String {
    let lm = lemma(what);
    serde_json::to_string(&lm).unwrap()
}

#[get("/enqueue/<url>")]
async fn enqueue_endpoint(url: &str) -> String {
    let db = open_db();
    if let Ok(mut todo_inserter) = db.inserter("force_query") {
        let mut is_ok = todo_inserter.write(&DbToScrap {
            url: url.to_string()
        }).is_ok();
        if !todo_inserter.end().await.is_ok() {
            is_ok = false;
        }
        if !is_ok {
            return "err".to_string();
        }
    }
    if let Ok(mut todo_inserter) = db.inserter("links") {
        let mut is_ok = todo_inserter.write(&DbLink {
            from_url: url.to_string(),
            to_url: url.to_string(),
        }).is_ok();
        if !todo_inserter.end().await.is_ok() {
            is_ok = false;
        }
        if !is_ok {
            return "err".to_string();
        }
    }
    "ok".to_string()
}

#[launch]
fn rocket() -> _ {
    rocket::build()
        .mount("/", routes![
               search_endpoint, 
               lemma_endpoint,
               enqueue_endpoint,
               complete_endpoint,
        ])
        .mount("/", FileServer::from(relative!("/static")))
}
