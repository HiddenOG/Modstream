"""Curated term lists used by the rule-based layer of the detector.

Three tiers:

* ``STRONG`` terms are unambiguous on their own (slurs, explicit threats,
  self-harm encouragement) and flag content without needing model agreement.
* ``CONTEXTUAL`` terms are insults that are also used innocently ("this test
  is stupid", news coverage of extremist groups). Alone they send content to
  human review; aimed at a person ("you're so stupid") they flag it.
* ``MILD`` terms are everyday profanity and words with innocent meanings
  ("that concert was fucking amazing", "the hoe is in the shed"). Alone they
  are allowed; aimed at a person, or stacked with another insult, they flag.

Purely religious or legal vocabulary (e.g. "sharia", "fatwa", "jihad") is
intentionally excluded: matching it penalises ordinary religious speech and
is a well-documented source of bias in moderation systems.
"""

STRONG = {
    "harassment": [
        "idiot", "moron", "retard", "loser", "bastard", "dumbass", "dipshit", "your fada",
        "kill yourself", "kys", "nobody likes you", "no one likes you", "go die", "jump off a bridge",
        "waste of space", "piece of shit", "eat shit", "shut the fuck up", "stfu", "gtfo", "fuck off",
        "slut", "whore", "skank",
    ],
    "profanity": ["cunt", "twat", "asshole", "motherfucker", "son of a bitch"],
    "hate": [
        "nigger", "chink", "gook", "kike", "spic", "paki", "coon", "jigaboo",
        "wog", "raghead", "beaner", "wetback", "sambo", "yellow peril",
        "white power", "go back to your country",
    ],
    "threat": [
        "i will kill you", "i'll kill you", "gonna kill you", "i will hurt you",
        "you will die", "shoot up the school",
    ],
    "extremism": [
        "suicide bomber", "suicide attack", "car bomb", "improvised explosive device",
        "behead", "holy war", "salafi jihadism",
    ],
}

CONTEXTUAL = {
    "harassment": [
        "stupid", "dumb", "ugly", "fool", "jerk", "lame", "suck", "mad", "craze",
        "punish", "freak", "pathetic", "worthless", "disgusting", "trash", "fat", "slob",
        "mumu", "olodo", "dumbo",
    ],
    "hate": ["cracker", "slant", "nazi", "fascist", "supremacist", "racist", "black power"],
    "threat": ["i know where you live", "watch your back"],
    "extremism": [
        "boko haram", "iswap", "ansaru", "al-qaeda", "al-shabaab", "isis", "isil",
        "aqim", "islamic state", "abubakar shekau", "jihadist", "mujahedeen",
        "terrorist", "terrorism", "extremist", "extremism", "insurgent", "insurgency",
        "militant", "militia", "gunman", "gunmen", "bomb", "bombing", "explosive",
        "ied", "grenade", "hostage", "kidnap", "kidnapping", "abduction", "massacre",
        "ambush", "assassinate", "weaponize", "martyrdom",
    ],
}

MILD = {
    "profanity": ["fuck", "shit", "bitch", "dick", "prick", "hoe", "screw", "crap"],
    "harassment": ["clown", "pig", "cow", "baby"],
}

# Common deliberate misspellings -> the canonical term they stand for. The
# matcher also tolerates stretched letters ("fuuuck"), masked letters ("f*ck"),
# spaced letters ("f u c k") and digit/symbol swaps ("b1tch", "$tupid").
VARIANTS = {
    "fuck": ["fuc", "fuk", "fck", "fcuk", "fvck", "phuck", "phuk", "fk", "fuq"],
    "shit": ["sht", "shyt", "shiet"],
    "bitch": ["biatch", "btch", "biotch"],
    "asshole": ["ahole", "arsehole"],
    "motherfucker": ["mf", "mofo", "mfer", "muthafucka", "motherfcker"],
    "kill yourself": ["kill urself", "kill yaself", "kill your self"],
}

# Categories whose single-word terms take inflections ("fucking", "fucker", "shitty").
INFLECTED = {"profanity", "harassment"}

# Words that aim an insult at the reader ("fuck you", "you're so dumb").
TARGETS = {
    "you", "u", "ya", "yu", "yall", "ur", "your", "youre", "yours", "yourself", "urself", "yaself",
    "thee", "thou",
}
# "your mom", "yo mama", "ur dad"...
FAMILY = {"mom", "mum", "mother", "mama", "momma", "dad", "father", "sister", "brother", "family", "wife"}
OWNERS = {"your", "ur", "yo", "ya"}
# First person: "I'm such an idiot" is self-talk, not bullying.
SELF = {"i", "im", "me", "myself"}

# "<subject> ... <violent verb> ... <target>" catches threats the fixed list doesn't.
THREAT_SUBJECTS = [
    "i will", "i'll", "ill", "i'm going to", "im going to", "i'm gonna", "im gonna", "gonna",
    "we will", "we'll", "we're going to", "were going to", "we're gonna",
]
VIOLENT_VERBS = {"kill", "hurt", "stab", "murder", "strangle", "punch", "rape", "bomb", "burn", "choke"}
