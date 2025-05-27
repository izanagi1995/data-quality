# Data-QA
* Last updated 2025-05-27 *

> **purpose** – This file is the onboarding manual for Claude and every human who edits this repository.  
> The goal is to specify the coding standards, the guard-rails and interactions between humans and AI in the coding process.

---

## 0. Project definition

Data-QA is a platform to assess and monitor Data Quality in complex Databricks environments. It provides Data Engineering teams a reliable and easy way to achieve stableness in quality. The *key components* in this root directory are :

- **lib** : This is the Python library based on Pyspark that will allow us to collect Data Quality Metrics from the Databricks Unity Catalog
- **backend** : This a `django-rest-framework` API that will collect the data from the lib and push them into a PostgreSQL database. It also handle authentication for the frontend and the API key generation for the lib authentication
- **frontend** : This is a React frontend that will allow the end user to see and configure its Data Quality Metrics

---

## 1. Non-negotiable golden rules

### a. What does the AI MAY do

1. The AI should ALWAYS the developer for clarification when the AI is unsure about the developer request.
2. Find the most relevant place to generate code.
3. Generate `AI-NOTE: about this code` comment style for non-trivial edited code.
4. If configured, use the installed linter and code style of the subdirectory you are working in.
5. If the change is more than 300 lines or more than 3 files, ask for confirmation.
6. Keep the current task context. If the developer asks you to do something that would span outside of the context, ask him to start a new conversation to reset the context. 

### b. What AI MUST NEVER do

1. The AI must never make a change without making sure of the project specifications and the feature specifications. If the specifications is unclear, please ask the developer.
2. Continue work from a prior prompt after "new task" – start a fresh session.
3. Refactor large portion of code without human guidance.

---

## 2. Coding standards

### `lib` directory
A Python library based on Pyspark. Never use pandas. Use `requests` for HTTP request. Easy interfaces (an http client for the backend, a class to collect Metrics from a single table...) for end user

### `backend` directory

Use best practices from Django and DRF to setup the project. Use models when required. Use Postgres best features. Create database indices when feeling necessary. 
Use models and request validators.
Use JWT for authentication in the frontend and API keys for the lib.
Document the endpoints with OpenAPI specs.

### `frontend` directory

A React app

### General guidance

Use try/catch or try/except structure when an error may arise.
When catching errors, never use generic error types. Use specific errors.

Always write docstrings for public resources.

## 3. Anchor comments

Add specially formatted comments throughout the codebase, where appropriate, for yourself as inline knowledge that can be easily `grep`ped for. 

### Guidelines:

- Use `AI-NOTE:`, `AI-TODO:`, or `AI-QUESTION:` (all-caps prefix) for comments aimed at AI and developers.
- Keep them concise (≤ 120 chars).
- **Important:** Before scanning files, always first try to **locate existing anchors** `AIDEV-*` in relevant subdirectories.
- **Update relevant anchors** when modifying associated code.
- **Do not remove `AIDEV-NOTE`s** without explicit human instruction.
- Make sure to add relevant anchor comments, whenever a file or piece of code is:
  * too long, or
  * too complex, or
  * very important, or
  * confusing, or
  * could have a bug unrelated to the task you are currently working on.

Example:
```python
# AIDEV-NOTE: perf-hot-path; avoid extra allocations (see ADR-24)
async def render_feed(...):
    ...
```

## 4. Commit discipline

*   **Granular commits**: One logical change per commit.
*   **Tag AI-generated commits**: e.g., `feat: optimise feed query [AI]`.
*   **Clear commit messages**: Explain the *why*; link to issues/ADRs if architectural.
*   **Use `git worktree`** for parallel/long-running AI branches (e.g., `git worktree add ../wip-foo -b wip-foo`).
*   **Review AI-generated code**: Never merge code you don't understand.

## 5. Implementing stories
When the developer ask you to implement a story, fetch the story content from github using `gh` cli. Create a new branch for this story.