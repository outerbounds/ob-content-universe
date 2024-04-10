import xml.etree.ElementTree as ET
import time
from typing import List
from datetime import datetime

import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm

OB_DOCS_SITE_SITEMAP = "https://outerbounds.com/sitemap.xml"
FIELDS = [
    "title",
    "content",
    "url",
    "date",
    "author",
    "tags",
    "canonical",
    "alternate",
    "description",
]


def get_urls_from_sitemap(sitemap_url: str = OB_DOCS_SITE_SITEMAP) -> List[str]:
    response = requests.get(sitemap_url)
    root = ET.fromstring(response.content)
    namespaces = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    urls = [
        url.text
        for url in root.findall("ns:url/ns:loc", namespaces=namespaces)
        if "blog" in url.text or "docs" in url.text or "engineering" in url.text
    ]
    return urls


def extract_data(urls: List[str]) -> pd.DataFrame:
    data = {field: [] for field in FIELDS}
    for url in tqdm(urls):
        page_response = requests.get(url)
        page_content = page_response.content
        soup = BeautifulSoup(page_content, "html.parser")
        head = soup.find("head")
        try:
            title = head.find("meta", attrs={"property": "og:title"}).get("content")
        except AttributeError:
            raise ValueError(f"og:title not found for {url}")
        try:
            description = head.find("meta", attrs={"property": "og:description"}).get(
                "content"
            )

        except AttributeError:
            raise ValueError(f"og:title not found for {url}")

        canonical = head.find("link", {"rel": "canonical"})["href"]
        alternate = head.find("link", {"rel": "alternate"})["href"]

        if "blog" in url:
            content = str(
                soup.find("div", id="post-content", class_="markdown firstLetter__Rps")
            )
            if content == "None":
                content = pd.NA
            try:
                tags = head.find("meta", attrs={"property": "article:tag"}).get(
                    "content"
                )
            except AttributeError:
                tags = pd.NA
            try:
                author = head.find("meta", attrs={"property": "article:author"}).get(
                    "content"
                )
            except:
                author = pd.NA
            try:
                publish_date = head.find(
                    "meta", attrs={"property": "article:published_time"}
                ).get("content")
            except:
                publish_date = pd.NA
        elif "docs" in url or "engineering" in url:
            content = str(soup.find("div", id="docMainContainer_gTbr"))
            if content == "None":
                content = str(soup.find("main"))
            if content == "None":
                raise ValueError(f"docMainContainer_gTbr not found for {url}")
            tags = ",".join(
                [
                    a_tag.text
                    for a_tag in soup.find_all("a", class_="tag_zVej tagRegular_sFm0")
                ]
            )
            author = pd.NA
            publish_date = pd.NA

        data["title"].append(title)
        data["content"].append(content)
        data["description"].append(description)
        data["url"].append(url)
        data["date"].append(publish_date)
        data["author"].append(author)
        data["canonical"].append(canonical)
        data["alternate"].append(alternate)
        data["tags"].append(tags)

    now = datetime.now()
    data["scrape_date"] = [now] * len(data["title"])
    return pd.DataFrame(data)
