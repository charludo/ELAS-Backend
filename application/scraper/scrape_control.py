import os
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
    subprocess.call(f"scrapy crawl course-catalog -a url='{e3_url}' -o {temp_e3}")

    os.chdir(config["ratingsScraper"])
    subprocess.call(f"scrapy crawl -a email=\"{config['ratingsEmail']}\" -a password=\"{config['ratingsPassword']}\" course-ratings -o {temp_ratings}")

    # 2. load the data from the temp files
    with open(temp_catalog) as file:
        insight_courses_raw = json.load(file)

    with open(temp_e3) as file:
        e3_courses_raw = json.load(file)

    with open(temp_ratings) as file:
        ratings = json.load(file)

    # 3. flatten the courses-files
    insight_courses = flatten(insight_courses_raw)
    e3_courses = flatten(e3_courses_raw)

    # 4. write courseInsights data to target file
    with open(config["courseInsightsTargetFile"], "w") as file:
        file.write(json.dump(insight_courses))

    # 5. process e3 data & ratings, write to target files
    e3_processed, avg_ratings = process_e3(e3_courses, ratings)

    with open(config["e3TargetFile"], "w") as file:
        file.write(json.dump(e3_processed))

    with open(config["e3RatingsFile"], "w") as file:
        file.write(json.dump(avg_ratings))

    # 6. update statusMessage in config
    config["statusMessage"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    with open(os.path.join(os.path.dirname(__file__), "config.yaml"), "w") as file:
        file.write(yaml.dump(config))


def flatten(unflat):
    if isinstance(unflat, list):
        for uf in unflat:
            yield from flatten(uf)
        return

    if "subjects" in unflat:
        for subject in unflat["subjects"]:
            yield subject

    if "categories" in unflat:
        for category in unflat["categories"]:
            yield from flatten(category)


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
            "Title": course["Title"],
            "catalog": course["catalog"],
            "Type": course["Type"],
            "SWS": course["SWS"],
            "Erwartete Teilnehmer": course["Erwartete Teilnehmer"],
            "Max. Teilnehmer": course["Max. Teilnehmer"],
            "Credits": course["Credits"],
            "Language": course["Language"],
            "Times_manual": course["times"],
            "Location": course["Location"],
            "Ausgeschlossen_manual": course["ausgeschlossen"],
            "Exam": course["Exam"],
            "Link": course["Link"]
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
