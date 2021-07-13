import os
import re
import yaml
import json
import subprocess
from datetime import datetime
from difflib import SequenceMatcher


def run(config, insight_url, e3_url):
    # 1. run all three scrapers: course-catalog for e3 and courseInsights, and course-ratings
    temp_catalog = os.path.abspath(os.path.join(os.path.dirname(__file__), "temp_catalog.json"))
    temp_e3 = os.path.abspath(os.path.join(os.path.dirname(__file__), "temp_e3.json"))
    temp_ratings = os.path.abspath(os.path.join(os.path.dirname(__file__), "temp_ratings.json"))

    os.chdir(config["courseScraper"])
    subprocess.call(f"scrapy crawl course-catalog -a url='{insight_url}' -o {temp_catalog}")
    subprocess.call(f"scrapy crawl course-catalog -a url='{e3_url}' -a e3=True -o {temp_e3}")

    os.chdir(config["ratingsScraper"])
    subprocess.call(f"scrapy crawl -a email=\"{config['ratingsEmail']}\" -a password=\"{config['ratingsPassword']}\" course-ratings -o {temp_ratings}")

    # 2. post-process and save the insight data
    os.chdir(os.path.join(config["courseScraper"], "course_catalog", "post_processing"))
    subprocess.call(f"python process_data.py {temp_catalog} {config['courseInsightsTargetFile']}")

    # 3. load the data from the temp files
    with open(temp_e3) as file:
        e3_courses = json.load(file)

    with open(temp_ratings) as file:
        ratings = json.load(file)

    # 4. process e3 data & ratings, write to target files
    e3_processed, avg_ratings = process_e3(e3_courses, ratings)

    with open(config["e3TargetFile"], "w") as file:
        file.write(json.dump(e3_processed))

    with open(config["e3RatingsFile"], "w") as file:
        file.write(json.dump(avg_ratings))

    # 5. update statusMessage in config
    config["statusMessage"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    with open(os.path.join(os.path.dirname(__file__), "config.yaml"), "w") as file:
        file.write(yaml.dump(config))


def process_e3(courses, ratings):
    processed_courses = []

    # Keeps track of number of ratings & sum total of rates
    avg_ratings = {
        "fairness": 0,
        "support": 0,
        "material": 0,
        "fun": 0,
        "comprehensibility": 0,
        "interesting": 0,
        "grade_effort": 0
    }
    ratings_count = 0

    for course in courses:
        # Rename the dict keys
        processed_course = {
            "selected": False,
            "Title": course["name"],
            "Link": course["url"],
            "catalog": course["parent_id"],
            "Type": course["subject_type"],
            "SWS": course["sws"],
            "Erwartete Teilnehmer": course["expected"],
            "Max. Teilnehmer": course["max"],
            "Credits": course["credits"],
            "Language": course["language"],
            "Times_manual": convert_timetable(course["timetable"]),
            "Location": get_locations(course["timetable"]),
            "Exam": get_exams(course["exam"]),
            "Ausgeschlossen_Ingenieurwissenschaften_Bachelor": get_excluded(course["excluded"])
        }

        # integrate the ratings, if they exist
        course_ratings = find_ratings(ratings, course["Title"])
        if course_ratings:
            # update the ratings tracker
            ratings_count += 1
            for key, item in avg_ratings.iter():
                avg_ratings[key] += course_ratings[key]

            processed_course = processed_course | course_ratings

        # append the processed course to the list
        processed_courses.append(processed_course)

    # calculate the average rating
    for key, item in avg_ratings.iter():
        avg_ratings[key] = item / ratings_count

    return processed_courses, avg_ratings


def find_ratings(ratings, title):
    for rating in ratings:
        similarity = SequenceMatcher(None, title, rating["name"]).ratio()
        if similarity > 0.75:
            return {
                "fairness": rating["fairness"] / 100,
                "support": rating["support"] / 100,
                "material": rating["material"] / 100,
                "fun": rating["fun"] / 100,
                "comprehensibility": rating["understandability"] / 100,
                "interesting": rating["interest"] / 100,
                "grade_effort": rating["node_effort"] / 100
            }
    return None


def convert_timetable(timetable):
    flattime = ""
    for dates in timetable:
        flattime += dates["day"][:-1] + dates["time"]["from"][:-3] + "-" + dates["time"]["to"][:-3] + ";"
    return flattime


def get_locations(timetable):
    locations = set()

    for date in timetable:
        loc = date["comment"]

        if "Dortmund" in loc:
            locations.add("Dortmund")
        elif "online" in loc:
            locations.add("online")
        elif any("Ruhr", "Bochum", "HNC", "RUB") in loc:
            locations.add("Bochum")
        elif "Essen" in loc or loc.startswith("E ") or loc.split(": ")[1].startswith("E "):
            locations.add("Essen")
        elif "Duisburg" in loc or loc.startswith("D ") or loc.split(": ")[1].startswith("D "):
            locations.add("Duisburg")

    if not len(locations):
        return "unknown"
    else:
        return ";".join(locations)


def get_exams(text):
    markers = {
        "Präsentation": [
            "referat", "präsentation", "presentation"
        ],
        "Mündliche Prüfung": [
            "mündlich", "oral", "prüfung"
        ],
        "Klausur": [
            "schriftlich", "klausur", "exam", "e-klausur", "präsenz", "written"
        ],
        "Essay": [
            "seitig", "page", "besprechung", "essay", "hausarbeit", "ausarbeitung", "seiten", "hausaufgabe", "dokumentation", "documentation", "protokoll",
            "zeichen", "character", "tagebuch", "diary", "assignment"
        ]
    }

    weight = {
        "Präsentation": 0,
        "Mündliche Prüfung": 0,
        "Klausur": 0,
        "Essay": 0
    }

    text = text.lower()

    for key, item in markers.iter():
        for marker in item:
            weight[key] += text.count(marker)

    if sum(weight.values()) == 0:
        return "unknown"

    return max(weight, key=lambda k: weight[k])


def get_excluded(text):
    shorthand = {
        "BauIng": "Bauingenieurwesen",
        "Komedia": "Komedia",
        "ISE": "ISE",
        "Maschinenbau": "Maschinenbau",
        "EIT": "Elektrotechnik und Informationstechnik",
        "Medizintechnik": "Medizintechnik",
        "NanoEng": "Nano Engineering",
        "Wi-Ing": "Wirtschaftsingenieurwesen",
        "Angewandte Informatik": "Angewandte Informatik",
        "IngWi": "ALLE",
        "Alle außer BauIng (1. FS)": "ALLE (außer Bauingenieurwesen (1. FS))",
        "IngWi (außer BauIng)": "ALLE (außer Bauingenieurwesen)"
    }

    text = re.sub(r"[^0-9a-zA-Z,.-]+", " ", text)

    excluded = []

    for key, item in shorthand.iter():
        if key in text:
            excluded.append(item)

    return ";".join(excluded) if len(excluded) else "-"
