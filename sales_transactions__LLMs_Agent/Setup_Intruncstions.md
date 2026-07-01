# Setup Instructions — Databricks MCP Server

This guide installs the [Databricks AI Dev Kit](https://github.com/databricks-solutions/ai-dev-kit)
(MCP server + agent skills for Claude Code, Codex, and Copilot) used across all
implementations in this repo.

## 1. Prerequisites

The installer checks for the following tools:

- ✅ git
- ✅ Databricks CLI (v1.1.0+)
- ✅ uv

### Install on Windows (PowerShell)

Using [winget](https://learn.microsoft.com/windows/package-manager/winget/):

```powershell
# git
winget install --id Git.Git -e

# Databricks CLI
winget install --id Databricks.DatabricksCLI -e

# uv (Python package/project manager)
irm https://astral.sh/uv/install.ps1 | iex
```

### Install on macOS / Linux (Bash)

macOS via [Homebrew](https://brew.sh/):

```bash
# git
brew install git

# Databricks CLI
brew tap databricks/tap
brew install databricks

# uv
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Linux (Debian/Ubuntu): `sudo apt-get install git`, install the
[Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html) per the
official docs, and install `uv` with the same `curl` command shown above.

### Verify the prerequisites

```bash
git --version
databricks --version
uv --version
```

### Authenticate the Databricks CLI

```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com
```

## 2. Install the MCP Server (AI Dev Kit)

**PowerShell (Windows)**

```powershell
irm https://raw.githubusercontent.com/databricks-solutions/ai-dev-kit/main/install.ps1 | iex
```

**Bash (macOS / Linux)**

```bash
curl -fsSL https://raw.githubusercontent.com/databricks-solutions/ai-dev-kit/main/install.sh | sh
```

## 3. Initialize the project

```bash
uv init
```

Set up `.gitignore`.

> ### ⚠️ MUST UPDATE BEFORE COMMIT
> Review `.gitignore` and any config so secrets / workspace details are not committed.

## 4. Run the process flow

### Test if the Databricks MCP Server is working

Prompt your agent:

```
List my warehouses
```

### Deploy the solution at Databricks

Go to agent / LLM model, in this example Opus 4.8 at SDP__Opus48 and 

Point the agent at an instruction file:

```
@Instrunctions.txt read the instructions and deploy the SDP solution at Databricks
```
#### To run your Experiment with the same setup

Create a new Catalog, Schema, and Volume, then upload a `.csv` file based on the sample archive provided at:

```
sales_transaction_data_raw/data/raw/sales_transactions.7z
```

Update the location in `Instructions.txt` for the model to be executed, then run the same command:

```
@Instructions.txt read the instructions and deploy the SDP solution on Databricks
```

### Prompt to create the README.md (example — Claude Opus 4.8)

```
create a README.md for this project and also include these numbers from chat history...
also include steps for how to trigger the pipeline after raw data ingestion and mask
personal info in the README.md.
- Detail how to execute using databricks cli and databricks sdk using python
```
