
use std::collections::HashMap;
use std::time;
use reqwest;
use json;
use simple_error::simple_error;
use futures::future::join_all;
use csv;
use serde::Serialize;

type RES<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;


#[derive(Serialize)]
struct Userdata
{
    rank: i32,
    username: String,
    country: String,
    ac_cnt_all: i32,
    company: String,
    school: String,
}


#[tokio::main]
async fn main()
{
    let dt = time::Instant::now();
    
    let res = go().await;
    if res.is_err() {
        println!("err = {}", res.err().unwrap());
    } else {
        println!("ok");
    }
    println!("dt = {}", dt.elapsed().as_millis());
}

async fn go() -> RES<()>
{
    let mut out = csv::Writer::from_path("rank.csv")?;
    
    let users = query_all_user_data().await?;
    for user in users {
        out.serialize(user)?;
    }
    out.flush()?;
    
    Ok(())
}

async fn query_all_user_data() -> RES<Vec<Userdata>>
{
    let page_cnt = 400;
    let mut users = Vec::new();
    users.resize_with(page_cnt, || Vec::new());
    
    let mut jobs = Vec::new();
    let mut page_num = 1;
    for user in users.iter_mut() {
        let job = query_rank_page(page_num, user);
        jobs.push(job);
        page_num += 1;
    }
    let results = join_all(jobs).await;
    for res in results {
        res?;
    }
    let mut users_flatten = Vec::new();
    for user_row in users {
        for user in user_row {
            users_flatten.push(user);
        }
    }
    Ok(users_flatten)
}

async fn query_rank_page(page_num: i32, users: &mut Vec<Userdata>) -> RES<()>
{
    let page = query_global_rank_from_leetcode(page_num).await?;
        
    for user_data in page["data"]["globalRanking"]["rankingNodes"].members() {
        let data = parse_user_data(user_data).await?;
        if data.is_none() {
            continue;
        }
        let data = data.unwrap();
        users.push(data);
    }
    query_all_user_detail_data(users).await?;
    Ok(())
}

async fn query_all_user_detail_data(users: &mut Vec<Userdata>) -> RES<()>
{
    let mut jobs = Vec::new();
    for user in users.iter_mut() {
        let job = query_user_detail_data_us(user);
        jobs.push(job);
    }
    let results = join_all(jobs).await;
    for res in results {
        if res.is_err() {
            println!("query_user_detail_data_us, err = {}", res.err().unwrap());
        }
    }
    Ok(())
}

async fn parse_user_data(user: &json::JsonValue) -> RES<Option<Userdata>>
{
    let data_region = user["dataRegion"].as_str().ok_or(simple_error!("data region is null"))?;
    if data_region == "US" {
        let rank = user["currentGlobalRanking"].as_i32().ok_or(simple_error!("user rank is null"))?;
        let username = user["user"]["username"].as_str().ok_or(simple_error!("username is null"))?.to_string();
        let country = user["user"]["profile"]["countryName"].as_str().unwrap_or("null").to_string();
        
        let data = Userdata {
            rank,
            username,
            country,
            ac_cnt_all: -1,
            company: "null".into(),
            school: "null".into()
        };
        Ok(Some(data))
    } else {
        Ok(None)
    }
}

async fn query_user_detail_data_us(data: &mut Userdata) -> RES<()>
{
    let user_data = query_user_data_from_leetcode(data.username.as_str()).await?;
    
    let company = user_data["data"]["matchedUser"]["profile"]["company"].as_str().unwrap_or("null");
    let school = user_data["data"]["matchedUser"]["profile"]["school"].as_str().unwrap_or("null");
    let mut ac_cnt = HashMap::new();
    for cnt_entry in user_data["data"]["matchedUser"]["submitStats"]["acSubmissionNum"].members() {
        let difficulty = cnt_entry["difficulty"].as_str().ok_or(simple_error!("difficulty is null"))?;
        let cnt = cnt_entry["count"].as_i32().ok_or(simple_error!("ac count is null"))?;
        ac_cnt.insert(difficulty, cnt);
    }
    let ac_cnt_all = ac_cnt.get("All").ok_or(simple_error!("ac cnt all is null"))?;
    
    data.ac_cnt_all = *ac_cnt_all;
    data.company = company.to_string();
    data.school = school.to_string();
    Ok(())
}

async fn query_global_rank_from_leetcode(page_num: i32) -> RES<json::JsonValue>
{
    let graph_ql  = r#"
        query getGlobalRanking ($page_num: Int) {
            globalRanking(page: $page_num) {
                rankingNodes {
                    ranking
                    currentRating
                    currentGlobalRanking
                    dataRegion
                    user {
                        username
                        profile {
                            countryCode
                            countryName
                            realName
                        }
                    }
                }
            }
        }
    "#;
    
    let body = json::object! {
        "operationName": null,
        "variables": {
            "page_num": page_num
        },
        "query": graph_ql
    };
    
    let res = query_graphql(body, format!("https://leetcode.com/contest/globalranking/{}/", page_num)).await?;
    Ok(res)
}

async fn query_user_data_from_leetcode(username: &str) -> RES<json::JsonValue>
{
    let graph_ql = r#"
        query getUserProfile($username: String!) {
            matchedUser(username: $username) {
                username
                profile {
                    realName
                    company
                    school
                    ranking
                }
                submitStats {
                    acSubmissionNum {
                        difficulty
                        count
                        submissions
                    }
                }
            }
        }
    "#;
    
    let body = json::object! {
        "operationName": "getUserProfile",
        "variables": {
            "username": username
        },
        "query": graph_ql
    };
    
    let res = query_graphql(body, format!("https://leetcode.com/{}/", username)).await?;
    Ok(res)
}


async fn query_graphql(graphql: json::JsonValue, referer: String) -> RES<json::JsonValue>
{
    let cln = reqwest::Client::new();
    let resp = cln.post("https://leetcode.com/graphql")
        .header("referer", referer)
        .header("cookie", "csrftoken=GoHXBSSY23446xYWWjLSfDYOFqxQPO7cPccscwxqbNyVOPn3AviQqeEr4RmOYqrk")
        .header("x-csrftoken", "GoHXBSSY23446xYWWjLSfDYOFqxQPO7cPccscwxqbNyVOPn3AviQqeEr4RmOYqrk")
        .header("content-type", "application/json")
        .body(graphql.to_string())
        .send().await?;
    
    let text = resp.text().await?;
    let j = json::parse(text.as_str())?;
    Ok(j)
}