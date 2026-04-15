import requests

PLAYLISTS = [
    "https://iptv-org.github.io/iptv/index.m3u",
    "https://iptv-org.github.io/iptv/languages/ara.m3u",
    "https://iptv-org.github.io/iptv/categories/sports.m3u"
]

OUTPUT_FILE = "clean_playlist.m3u"


def download(url):
    try:
        return requests.get(url, timeout=10).text
    except:
        return ""


def clean_and_filter(data):
    lines = data.split("\n")
    output = []

    for i in range(len(lines)):
        if lines[i].startswith("#EXTINF") and i+1 < len(lines):
            name = lines[i].lower()
            stream = lines[i+1]

            # فلترة ذكية
            if any(x in name for x in ["arab", "sport", "news", "movie"]):
                if stream.startswith("http"):
                    output.append(lines[i])
                    output.append(stream)

    return output


def main():
    final = ["#EXTM3U"]

    for url in PLAYLISTS:
        print("Processing:", url)
        data = download(url)
        final.extend(clean_and_filter(data))

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(final))

    print("✅ DONE:", OUTPUT_FILE)


if __name__ == "__main__":
    main()
