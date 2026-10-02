"""Curated term lists used by the rule-based layer of the detector.

Terms are split into two tiers:

* ``STRONG`` terms are unambiguous on their own (slurs, explicit threats,
  self-harm encouragement) and flag content without needing model agreement.
* ``CONTEXTUAL`` terms are frequently used innocently ("heart attack",
  "this test is stupid", news coverage of extremist groups). On their own
  they only send content to human review; they escalate to a flag when the
  ML model also finds the text toxic.

Purely religious or legal vocabulary (e.g. "sharia", "fatwa", "jihad") is
intentionally excluded: matching it penalises ordinary religious speech and
is a well-documented source of bias in moderation systems.
"""

STRONG = {
    "harassment": [
        "idiot", "moron", "retard", "loser", "bastard", "your fada",
        "kill yourself", "kys", "nobody likes you", "go die",
    ],
    "profanity": ["cunt", "twat", "bitch", "asshole"],
    "hate": [
        "nigger", "chink", "gook", "kike", "spic", "paki", "coon", "jigaboo",
        "wog", "raghead", "beaner", "wetback", "sambo", "yellow peril",
        "white power",
    ],
    "threat": [
        "i will kill you", "i'll kill you", "gonna kill you", "i will hurt you",
        "you will die",
    ],
    "extremism": [
        "suicide bomber", "suicide attack", "car bomb", "improvised explosive device",
        "behead", "holy war", "salafi jihadism",
    ],
}

CONTEXTUAL = {
    "harassment": [
        "stupid", "dumb", "ugly", "fool", "jerk", "lame", "suck", "mad", "craze",
        "punish",
    ],
    "profanity": ["fuck", "shit", "damn"],
    "hate": ["cracker", "slant", "nazi", "fascist", "supremacist", "racist", "black power"],
    "extremism": [
        "boko haram", "iswap", "ansaru", "al-qaeda", "al-shabaab", "isis", "isil",
        "aqim", "islamic state", "abubakar shekau", "jihadist", "mujahedeen",
        "terrorist", "terrorism", "extremist", "extremism", "insurgent", "insurgency",
        "militant", "militia", "gunman", "gunmen", "bomb", "bombing", "explosive",
        "ied", "grenade", "hostage", "kidnap", "kidnapping", "abduction", "massacre",
        "ambush", "assassinate", "weaponize", "martyrdom",
    ],
}
