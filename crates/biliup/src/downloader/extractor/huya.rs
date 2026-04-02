use crate::client::StatelessClient;
use crate::downloader::error::Result;
use crate::downloader::extractor::{Extension, Site, SiteDefinition};
use async_trait::async_trait;
use serde_json::Value;
use std::any::Any;

pub struct HuyaLive {}

#[async_trait]
impl SiteDefinition for HuyaLive {
    fn can_handle_url(&self, url: &str) -> bool {
        match regex::Regex::new(r"(?:https?://)?(?:(?:www|m)\.)?huya\.com") {
            Ok(re) => re.is_match(url),
            Err(_) => false,
        }
    }

    async fn get_site(&self, url: &str, client: StatelessClient) -> Result<Site> {
        let response = client.client.get(url).send().await?;
        // println!("{:?}", response);

        let text = response.text().await?;
        let mut stream: Value = match regex::Regex::new(r"stream: (\{.+)\n.*?\};") {
            Ok(re) => match re.captures(&text) {
                Some(captures) => serde_json::from_str(&captures[1])?,
                _ => {
                    return Err(crate::downloader::error::Error::Custom(format!(
                        "Not online: {text}"
                    )));
                }
            },
            Err(_) => {
                return Err(crate::downloader::error::Error::Custom(format!(
                    "Not online: {text}"
                )));
            }
        };
        let game = stream["data"][0].take();
        let game_stream_info = game["gameStreamInfoList"]
            .as_array()
            .and_then(|game_stream_info_list| game_stream_info_list.first())
            .ok_or_else(|| {
                crate::downloader::error::Error::Custom(format!("Not online: {game}"))
            })?;
        let mut v_multi_stream_info = stream["vMultiStreamInfo"].take();
        // vec![1,2].iter().max()
        // println!("{}", v_multi_stream_info);
        let _stream_info = v_multi_stream_info
            .as_array()
            .and_then(|v| v.iter().max_by_key(|info| info["iBitRate"].as_i64()));
        // println!("{:?}", stream_info);
        // let ratio = ;
        let direct_url = format!(
            "{}/{}.{}?{}&ratio={}",
            game_stream_info["sFlvUrl"].as_str().ok_or_else(|| {
                crate::downloader::error::Error::Custom(format!(
                    "Missing flv url: {game_stream_info}"
                ))
            })?,
            game_stream_info["sStreamName"].as_str().ok_or_else(|| {
                crate::downloader::error::Error::Custom(format!(
                    "Missing stream name: {game_stream_info}"
                ))
            })?,
            game_stream_info["sFlvUrlSuffix"].as_str().ok_or_else(|| {
                crate::downloader::error::Error::Custom(format!(
                    "Missing url suffix: {game_stream_info}"
                ))
            })?,
            game_stream_info["sFlvAntiCode"].as_str().ok_or_else(|| {
                crate::downloader::error::Error::Custom(format!(
                    "Missing anti code: {game_stream_info}"
                ))
            })?,
            v_multi_stream_info[0]["iBitRate"].take()
        );
        // println!("{}", direct_url);
        Ok(Site {
            name: "huya",
            title: game["gameLiveInfo"]["introduction"]
                .as_str()
                .ok_or_else(|| {
                    crate::downloader::error::Error::Custom(format!("Missing title: {game}"))
                })?
                .to_string(),
            direct_url,
            extension: Extension::Flv,
            client,
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
