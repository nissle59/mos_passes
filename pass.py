import asyncio
import logging
import subprocess
import traceback
from pathlib import Path
import requests
#from auth import AuthEMU
import warnings
import db
import config


LOGGER = logging.getLogger(__name__)
warnings.filterwarnings("ignore")

Path('sql').mkdir(exist_ok=True)
#Path('../fails').mkdir(exist_ok=True)


def run_command(command):
    std = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return std.stdout.decode()


def docker_run(image, env: dict | None = None, command: str | None = None, autoremove_container = True):
    s = 'docker run '
    if env:
        for e in env:
            s += f'-e {e}=\'{env[e]}\' '
    if autoremove_container:
        s += f'--rm {image}'
    else:
        s += f'{image}'
    if command:
        s += f' {command}'
    print(s)
    std = run_command(s)
    return std


class MosPass:
    def __init__(self, username, password):
        self.total_passed = 0
        self.fails = 0
        self.username = username
        self.password = password
        self.session = requests.Session()
        try:
            cv = asyncio.run(db.get_account(username))[0]['cookie_value']
            print(cv)
            self.cookies = {
                ".AspNetCore.Cookies": cv
            }
        except Exception as e:
            traceback.print_exc()
            self.cookies = None

    def auth(self):
        cv = docker_run(config.AUTH_IMAGE, {'USERNAME': self.username, 'PASSWORD': self.password})
        # cv = run_command(
        #     f"docker run -e USERNAME '{self.username}' -e PASSWORD '{self.password}' --rm {config.AUTH_IMAGE}")
        self.cookies = {
            ".AspNetCore.Cookies": cv
        }
        # self.a = AuthEMU(self.username, self.password)
        # self.cookies = self.a.auth()
        db.set_account(self.username, self.password, cv)
        # json.dump(self.cookies, open(Path('cookies') / Path(f'{self.username}_cookies.json'), 'w'))

    def get_pass_info(self, pass_no: str) -> dict | None:
        headers = {
            "Sec-Ch-Ua": "\"Not/A)Brand\";v=\"8\", \"Chromium\";v=\"126\"",
            "Accept": "application/json",
            "Accept-Language": "ru-RU",
            "Sec-Ch-Ua-Mobile": "?0",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.6478.127 Safari/537.36",
            "Sec-Ch-Ua-Platform": "\"macOS\"",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Referer": "https://lk.ovga.mos.ru/vehicle-pass-requests/create",
            "Accept-Encoding": "gzip, deflate, br",
            "Priority": "u=1, i",
            "Connection": "keep-alive"
        }
        params = {
            "SeriesAndNumber": pass_no.replace(" ", "")
        }
        if not self.cookies:
            # sys.exit(1)
            self.auth()
        r = self.session.get(
            url=f"https://lk.ovga.mos.ru/api/Pass/GetPassBySeriesAndNumber",
            params=params,
            headers=headers,
            cookies=self.cookies,
            verify=False
        )
        status = r.status_code
        # print(r.url)
        # print(status)
        # print(r.headers)
        # print(r.text)
        if status == 200:
            self.total_passed += 1
            self.fails = 0
            return r.json()
        elif status in [404, 400]:
            self.total_passed += 1
            self.fails = 0
            return None
        elif status == 401:
            self.fails += 1
            print("Unauthorized")
            # sys.exit(1)
            self.auth()
            if self.fails <= 10:
                return self.get_pass_info(pass_no)
            else:
                LOGGER.critical(f"Too much fails for {pass_no}, account {self.username}, total passed: {self.total_passed}")


if __name__ == "__main__":
    pmos = MosPass('nixncom@gmail.com', 'qAzWsX159$$$1')
    start_n= 1670000
    stop_n = 1000000
    for i in range(start_n, stop_n, -1):
        s = str(i)
        while len(s) < 7:
            s = '0' + s
        stat = pmos.get_pass_info(f"БА {s}")
        if stat:
            if isinstance(stat, dict):
                LOGGER.info(f"{pmos.total_passed} --- БА {s}: {stat['vin']} :: {stat['regNum']} :: {stat['statusCode']}")
                asyncio.run(db.set_pass(stat))
        else:
            LOGGER.info(f'{pmos.total_passed} --- БА {s} Не существует')
