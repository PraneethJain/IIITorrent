import subprocess
import time

def send_requests (magnet_link: str):
    subprocess.Popen(["python3", "./torrent_cli.py", "start"], close_fds = False)

    time.sleep(2)
    
    subprocess.Popen(["python3", "./torrent_cli.py", "add", magnet_link])

    time.sleep(2)
    
    read = 0.0
    while read < 1.0:
        status = subprocess.Popen(["python3", "./torrent_cli.py", "status-machine"], stdout=subprocess.PIPE)
        out, _ = status.communicate(timeout=1)

        print(out.split(b'\n'))
        read = float(out.split(b'\n')[1])

    time.sleep(2)

    subprocess.Popen(["python3", "./torrent_cli.py", "remove", magnet_link])

    time.sleep(2)
    
    subprocess.Popen(["python3", "./torrent_cli.py", "stop"])

    time.sleep(2)

if __name__ == '__main__':
    magnet_link = """magnet:?xt=urn:btih:a88fda5954e89178c372716a6a78b8180ed4dad3&dn=The+WIRED+CD+-+Rip.+Sample.+Mash.+Share&tr=udp%3A%2F%2Fexplodie.org%3A6969&tr=udp%3A%2F%2Ftracker.coppersurfer.tk%3A6969&tr=udp%3A%2F%2Ftracker.empire-js.us%3A1337&tr=udp%3A%2F%2Ftracker.leechers-paradise.org%3A6969&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337&tr=wss%3A%2F%2Ftracker.btorrent.xyz&tr=wss%3A%2F%2Ftracker.fastcast.nz&tr=wss%3A%2F%2Ftracker.openwebtorrent.com&ws=https%3A%2F%2Fwebtorrent.io%2Ftorrents%2F&xs=https%3A%2F%2Fwebtorrent.io%2Ftorrents%2Fwired-cd.torrent"""

    magnet_link = "./rickroll-2.torrent"
    send_requests(magnet_link)
