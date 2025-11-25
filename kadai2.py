import requests
from bs4 import BeautifulSoup

url = "https://news.yahoo.co.jp/"
html = requests.get(url).text
soup = BeautifulSoup(html, "html.parser")

title = soup.find("a").get_text()
print("スクレイピング成功:", title)
