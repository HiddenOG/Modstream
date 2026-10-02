from flask import Flask, request, render_template, redirect, url_for, Response, jsonify
from detoxify import Detoxify
from transformers import pipeline
from werkzeug.utils import secure_filename
import os
import json
import time

# --- Flask setup ---
app = Flask(__name__)
UPLOAD_FOLDER = "static/uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# --- In-memory storage ---
posts = []  # Each post: {id, author, text, image, likes, shares, comments, flagged}

# --- Bad word lists ---
TERROR_WORDS = [
    "boko haram", "iswap", "ansaru", "jihad", "terrorist", "terrorism", "extremism", "radicalization",
    "suicide bomber", "suicide attack", "bombing", "explosion", "car bomb", "improvised explosive device", "IED",
    "hostage", "kidnapping", "abduction", "massacre", "insurgency", "militant", "militancy", "extremist",
    "sharia law", "caliphate", "jihadist", "martyrdom", "martyr", "holy war", "holy struggle", "fatwa",
    "radical islam", "wahhabism", "salafism", "salafi jihadism", "islamic state", "caliphate", "khilafah",
    "wilayat", "shura", "mujahedeen", "mujaheddin", "al-qaeda", "al-shabaab", "isis", "isil", "isil-wap",
    "boko haram", "islamic state", "abubakar shekau", "terrorist", "jihad", "bomber",
    "suicide bomber", "attack", "bomb", "insurgent", "radical", "militant", "kidnap",
    "abduction", "hostage", "explosive", "IED", "ambush", "massacre", "raids", 
    "extremist", "sharia", "jihadi", "caliphate", "martyrdom", "behead", "kill", 
    "assassinate", "firefight", "gunman", "armament", "weaponize", "grenade", 
    "militia", "gunfight", "raid", "gunmen", "insurgency", "terror",
    "al-qaeda in the islamic maghreb", "aqim", "boko haram insurgency", "boko haram attack", "boko haram bombing",
    "boko haram kidnapping", "boko haram abduction", "boko haram massacre", "boko haram suicide", "boko haram raid",
    "boko haram militants", "boko haram fighters", "boko haram extremists", "boko haram ideology", "boko haram sharia",
    "boko haram caliphate", "boko haram propaganda", "boko haram recruitment", "boko haram training", "boko haram weapons",
    "boko haram explosives", "boko haram tactics", "boko haram ambush", "boko haram raid", "boko haram attack plan",
    "boko haram sleeper cell", "boko haram sleeper agent", "boko haram sleeper network", "boko haram sleeper cell"]

RACIST_WORDS = [
    "nigger", "chink", "gook", "kike", "spic", "paki", "coon", "jigaboo", "wog",
    "cracker", "raghead", "beaner", "wetback", "nazi", "fascist", "supremacist",
    "racist", "racism", "white power", "black power", "nigger", "chink", "gook", "kike", "spic", "paki", "coon", "jigaboo", "wog", 
    "slant", "yellow peril", "sambo", "cracker", "raghead", "beaner", "wetback",
    "nazi", "fascist", "supremacist", "racist", "racism", "white power", "black power",
    "ethnic slur", "racial slur", "racist joke", "racist meme"
]

BAD_WORDS = ["stupid", "idiot", "dumb", "fuck", "shit", "bitch", "loser", "moron", "jerk", 
    "asshole", "suck", "ugly", "fool", "lame", "twat", "cunt", "damn", "retard", 'craze', 'mad', 'punish', 'your fada', 'bastard'] + TERROR_WORDS + RACIST_WORDS

# --- Load models ---
detoxify_model = Detoxify('original')
toxic_model = pipeline("text-classification", model="unitary/toxic-bert")


def process_comment(comment):
    text = comment.lower()
    bad_flags = [word for word in BAD_WORDS if word in text]
    detox_scores = detoxify_model.predict(text)
    detox_top = max(detox_scores, key=detox_scores.get)
    detox_conf = float(detox_scores[detox_top])

    if bad_flags or detox_conf > 0.7:
        status = "flagged"
        reason = "bad words" if bad_flags else "detoxify"
    else:
        status = "safe"
        reason = None

    return {
        "status": status,
        "reason": reason,
        "bad_flags": bad_flags,
        "detox_top": detox_top,
        "detox_confidence": round(detox_conf, 3)
    }
# --- Routes ---

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/help")
def help_page():
    return render_template("help.html")

# Paste Text Page
@app.route("/paste", methods=["GET","POST"])
def paste_page():
    result = None
    if request.method == "POST":
        comment = request.form.get("comment", "")
        result = process_comment(comment)
    return render_template("paste.html", result=result)

# Mini Social Media Page
@app.route("/facebook", methods=["GET","POST"])
def facebook_page():
    if request.method == "POST":
        author = request.form.get("author", "Anonymous")
        text = request.form.get("text", "")
        image = request.files.get("image")
        filename = None
        if image and image.filename:
            filename = secure_filename(image.filename)
            image.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))

        result = process_comment(text)
        flagged = result["status"] == "flagged"

        post = {
            "id": len(posts)+1,
            "author": author,
            "text": text,
            "image": filename,
            "likes": 0,
            "shares": 0,
            "comments": [],
            "flagged": flagged,
            "result": result
        }
        posts.append(post)

        # If flagged, you could send a message back (e.g., flash or alert)
        return redirect(url_for("facebook_page"))

    return render_template("facebook.html", posts=posts)

@app.route("/facebook/comment/<int:post_id>", methods=["POST"])
def add_comment(post_id):
    author = request.form.get("author", "Anonymous")
    text = request.form.get("comment", "")
    result = process_comment(text)
    flagged = result["status"] == "flagged"

    comment = {"author": author, "text": text, "flagged": flagged, "result": result}

    for post in posts:
        if post["id"] == post_id:
            post["comments"].append(comment)
            break
    return redirect(url_for("facebook_page"))

# --- Likes / Shares ---
@app.route("/facebook/like/<int:post_id>")
def like_post(post_id):
    for post in posts:
        if post["id"] == post_id:
            post["likes"] += 1
            break
    return redirect(url_for("facebook_page"))

@app.route("/facebook/share/<int:post_id>")
def share_post(post_id):
    for post in posts:
        if post["id"] == post_id:
            post["shares"] += 1
            break
    return redirect(url_for("facebook_page"))

# --- Live Feed SSE ---
@app.route("/facebook/stream")
def facebook_stream():
    def event_stream():
        last_index = 0
        last_comments_len = [0]*len(posts)
        while True:
            # New posts
            if len(posts) > last_index:
                for p in posts[last_index:]:
                    yield f"data: {json.dumps({'type':'post','post':p})}\n\n"
                last_index = len(posts)
                last_comments_len.extend([0]*(len(posts)-len(last_comments_len)))
            # New comments
            for i, p in enumerate(posts):
                if len(p["comments"]) > last_comments_len[i]:
                    for c in p["comments"][last_comments_len[i]:]:
                        yield f"data: {json.dumps({'type':'comment','post_id':p['id'],'comment':c})}\n\n"
                    last_comments_len[i] = len(p["comments"])
            time.sleep(1)
    return Response(event_stream(), mimetype="text/event-stream")

# --- Feed Page ---
@app.route("/facebook/live")
def feed_page():
    return render_template("feed.html")

# --- Actions Page ---
@app.route("/actions")
def actions_page():
    return render_template("actions.html")

# --- Chatbot Page ---
@app.route("/chatbot", methods=["GET","POST"])
def chatbot_page():
    if request.method=="GET":
        return render_template("chatbot.html")
    data = request.get_json()
    msg = data.get("message","")
    result = process_comment(msg)
    flagged = result["status"] == "flagged"
    if flagged:
        bot_reply = "⚠ Warning: Your message may contain harmful content."
    else:
        bot_reply = "Message is safe."
    return jsonify({"reply": bot_reply, "flagged": flagged})

if __name__=="__main__":
    app.run(debug=True, threaded=True)




