# use python3 to run
import json

from bs4 import BeautifulSoup
from urllib.parse import urljoin


rainbow_base_url = 'https://bigdata-view.cootekservice.com:50040/r/'


def log_in_rainbow(rainbow_session, user, pwd):
    head = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"}

    data = {
        'UserName': user,
        'Password': pwd,
        'AuthMethod': 'FormsAuthentication'
    }
    soup = BeautifulSoup(rainbow_session.get('https://bigdata-view.cootekservice.com:50040/').content, 'lxml')
    redir = soup.select_one("#loginForm")["action"]
    url = 'https://sso.corp.cootek.com/'
    rainbow_session.post(urljoin(url, redir), data=data, headers=head)


def pull_rainbow_raw_data(session, url):
    f = session.get(url)
    pull_data_url = f.url.replace('explore', 'explore_json').replace('/spa', '')
    content = json.loads(session.get(pull_data_url).content.decode())
    if not content or not content['data']:
        return []
    raw_data = content['data']['records']
    return raw_data
