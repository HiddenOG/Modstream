# Evaluation

How well does the detector actually work? [`dataset.jsonl`](dataset.jsonl) holds 179 hand-labelled
messages (107 harmful, 72 safe) across 12 categories, built to probe specific weaknesses:

| Harmful | Safe |
|---|---|
| direct insults, swearing aimed at a person, disguised spellings, threats, self-harm encouragement, hate speech, insults with no bad words | everyday chat, harmless swearing, innocent lookalike words ("Scunthorpe", "mad respect"), news and counter-speech, friendly banter |

```bash
python evaluation/run_eval.py --errors                 # rules only
python evaluation/run_eval.py --scorer detoxify        # rules + AI model
```

## Metrics

| Metric | Meaning | Want |
|---|---|---|
| Catch rate | harmful messages flagged | high |
| Sent to review | harmful messages flagged *or* sent to a human | high |
| False alarms | safe messages flagged | low |
| Review load | safe messages sent to review (not wrong, but costs moderator time) | low |

A filter that flags everything has a 100% catch rate and is useless, so catch rate and false
alarms are always reported together.

## Results

| Version | Catch rate | Sent to review | False alarms | Review load | Speed |
|---|---:|---:|---:|---:|---:|
| Rules v1 (word lists) | 28.0% | 45.8% | 5.6% | 26.4% | 0.055 ms |
| **Rules v2** (targeting, disguises, threat pattern) | **74.8%** | **80.4%** | **5.6%** | **18.1%** | 0.132 ms |

Per category, rules v1 → v2:

| Category | v1 caught | v2 caught |
|---|---:|---:|
| Swearing aimed at a person ("fuck your mother") | 13% | **100%** |
| Disguised spellings ("fuc", "f*ck", "f u c k", "b1tch") | 28% | **96%** |
| Direct insults | 36% | **92%** |
| Threats | 25% | 58% |
| Insults with no bad words ("no one will ever love you") | 0% | 10% |
| Harmless swearing flagged or sent to review (lower is better) | 58% | **0%** |

Raw outputs: [`results/`](results/).

## What changed in rules v2

1. **Targeting.** Insults and profanity aimed at a person ("fuck you", "your mom is a...", "you're so
   stupid") or stacked together ("stupid bitch") are flagged. The same words not aimed at anyone
   ("that concert was fucking amazing") are allowed.
2. **Disguise resistance.** Stretched letters, masked letters (`f*ck`), spaced letters (`f u c k`),
   digit and symbol swaps (`b1tch`, `$tupid`, `sh!t`) and known misspellings (`fuc`, `phuck`), with
   highlights still pointing at the text the user actually typed.
3. **Self-talk.** "I'm such an idiot" goes to review instead of being flagged.
4. **Threat pattern.** "I'll / gonna / we're going to" + a violent verb + "you" catches threats the
   fixed phrase list doesn't. Ambiguous verbs ("beat", "shoot", "destroy") are left out because
   "I'll beat you at FIFA" is banter.

## Honest limitations

- **The same person wrote the rules and the test set.** The set was written and committed *before*
  the rule changes (see git history), and the rules follow general principles rather than test
  sentences, but some stock phrases ("waste of space", "jump off a bridge") appear in both. Treat
  these numbers as optimistic. The categories the rules were not designed for, such as insults with
  no bad words, are the more honest signal.
- **What rules can't do:** understand meaning. Almost every remaining miss is an insult with no
  keywords, a self-harm suggestion phrased politely, or hate speech without slurs. That's the AI
  model's job; it gets measured on this same set next.
- **Counter-speech:** "If someone tells you to kill yourself, report it" is still flagged. Quoting
  abuse in order to condemn it is a known hard problem.
- 179 messages is enough to compare versions, not to estimate real-world accuracy precisely.
