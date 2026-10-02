Cyberbully-detector/        #folder
│
├─ app.py                   # Main Flask app
├─ requirements.txt         # Python dependencies
├─ README.md                # Setup instructions
├─ templates/               # HTML templates
│   ├─ index.html
│   ├─ paste.html
│   ├─ facebook.html
│   ├─ feed.html
│   └─ actions.html
├─ static/                  # CSS, JS, and uploaded images
│   ├─ style.css
│   └─ uploads/             # Empty folder, Flask will save uploads here

# CyberShield - Cyberbullying & Terrorism Detector

## Overview
Cybershield is a Flask web application that detects harmful comments online. 
It uses:
- **Detoxify** model for toxicity detection.
- **Toxic-BERT** from HuggingFace for offensive content.
- Custom word lists for terrorism-related and racist content.

The app includes:
- Paste page to check comments.(User copies and paste the comments to see if its toxic or not)
- Mini social feed where posts and comments are flagged.
- Live monitoring feed.(monitors the activities of comments publicly to make sure harmful comments are flagged live)
- Recommended actions page.

---

## Setup Instructions

1. **Download or unzip the project folder.**

2. **Ensure Python 3.9+ is installed.**

3. **Open command prompt**

4. **Open the command prompt in the project folder: cd path/to/your/project**

5. **create virtual environment:python -m venv venv**

6. **Activate the environment:venv\Scripts\activate**

7. **Install dependencies: pip install requirements.txt**

8. **Open the Vscode in the folder location**

9. Run the Flask app:

python app.py

Open in your browser:

http://127.0.0.1:5000/

Notes

Uploaded images will be stored in static/uploads/.

All pages include a navbar to navigate back to Home.

The Paste page can detect toxic, terrorist, or racist content
