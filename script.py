import requests

PLAYLISTS = [
    "https://iptv-org.github.io/iptv/index.m3u",
    "https://iptv-org.github.io/iptv/languages/ara.m3u",
    "https://iptv-org.github.io/iptv/categories/sports.m3u"
]

OUTPUT_FILE = "clean_playlist.m3u"


def download_playlist(url):
    try:
        response = requests.get(url, timeout=10)
        return response.text
    except:
        return ""


def is_working(url):
    try:
        r = requests.head(url, timeout=5)
        return r.status_code == 200
    except:
        return False


def process_playlist(data):
    lines = data.split("\n")
    clean = []

    for i in range(len(lines)):
        if lines[i].startswith("#EXTINF") and i+1 < len(lines):
            stream = lines[i+1]

            if stream.startswith("http"):
                if is_working(stream):
                    clean.append(lines[i])
                    clean.append(stream)

    return clean


def main():
    final_playlist = ["#EXTM3U"]

    for url in PLAYLISTS:
        print(f"Processing: {url}")
        data = download_playlist(url)
        cleaned = process_playlist(data)
        final_playlist.extend(cleaned)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(final_playlist))

    print("✅ Clean playlist saved as:", OUTPUT_FILE)


if __name__ == "__main__":
    main()
