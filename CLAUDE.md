# CLAUDE.md

Saare rules `AGENTS.md` mein hain. Unko hamesha follow karo:

@AGENTS.md

## Claude ke liye yaad dihani
- Har session ke shuru mein `phases.md` dekho ke hum kahan hain.
- User **seekh raha hai**: har code, har faisla, Roman Urdu mein aasan alfaaz aur examples ke saath samjhao.
- Shell: Windows (PowerShell / Git Bash). Infra Docker Compose par hai. LLM cloud par hai (Groq), local nahi.

- Never ever change the tests, change the code to make sure that test is passed. If fail do not change the test to make it pass.
- Make sure to fail the test first, fix the implementation at scalable level and make sure architecture implementation remains clean.