---

kanban-plugin: board

---

## P1

- [ ] Add log generators for all integrations
- [ ] [Incorporate ML datagen](https://github.com/noMoreCLI/ml-webvisit-metric-gen)
- [ ] [Trace/metric/log correlation](https://elastic.slack.com/archives/C0AKHB2QQNB/p1779282005986229)
- [ ] Integrate [streams_messy_logs](https://github.com/roshan-elastic/streams_messy_logs)?
- [ ] Docs Improvements
	- [ ] Dashboard conversation
	- [ ] Interesting PromQL Queries
- [ ] Survey link that allows the audience to vote for the failure scenario
- [ ] RUM
- [ ] K8s
	  - [ ] What's needed for the upcoming k8s content


## P2

- [ ] Improve dashboard appeal (Jen)
- [ ] When the channel fault times out, the case is left open.
- [ ] Add Claude Code and VSCode to the demo.
- [ ] Ability to add scenario after demo is launched
	  - [ ] Ability to troubleshoot using Claude + MCP
- [ ] Infrastructure spikes
	  - [x] Infrastructure spike toggles are hard to see
	  - [ ] Can they be made to be channel specific?
- [ ] DB OTel Generator
	  - [ ] [Code](https://github.com/poulsbopete/dbmonitoring/blob/main/tools/db_otel_generator.py) [Slack Thread](https://elastic.slack.com/archives/C0JFHDHRC/p1774875620944659)
- [ ] RUM? https://www.elastic.co/docs/solutions/observability/applications/otel-rum


## P3

- [ ] Instruqt version for customer-facing delivery
	  - [ ] [Instruqt Demo](https://play.instruqt.com/manage/elastic-pmm/tracks/nova-launch-demo)
	  - [ ] Target: landing page, chaos controller, and serverless all on one VM
	  - [ ] Goal: load time under 3 minutes (Peter)
	  - [ ] Fix executive dashboard link and other links to Kibana
	  - [ ] Write the script
- [ ] Kiosk Mode
	  - [ ] Always have 2 channels active.  I.e. when channel 1 gets remediated, trigger channel 3.
- [ ] Synthetic monitors
	  - [ ] Add some synthetic monitors to identify failures
- [ ] Dashboard page in Elastic instead of this app
	  - [x] Drop the current non-Elastic dashboard (Peter)
	  - [ ] Build a high-level service dashboard in Elastic — not too busy (Alexander)
	  - [ ] Use Vega for visualizations — both Peter and Alexander are enthusiastic about this
	  - [ ] Traffic light indicators (green/yellow/red) for critical services and infrastructure (Alexander)
	  - [ ] Drilldown from the high-level view to the existing detailed dashboard (Alexander)
	  - [ ] Think ITSI-lite: simpler and less overloaded than what exists today
	  - [ ] Visual target: Splunk-style "eyes catching" traffic light aesthetic (Alexander, Mar 17)
	  - [x] Delete old dashboard still available at /dashboard?deployment_id=space


## Complete

- [x] Update existing log generators to trigger OTel Integration install and ensure dashboards work
- [x] ML Jobs
	  - [x] [Add additional ML-based jobs to showcase](https://elastic.slack.com/archives/C0AKHB2QQNB/p1778079678880089) — tied to the o11y "faster RCA with AI-powered features" slide; ideally one demo per AI Ops feature on that slide (currently only APM ML jobs exist). Note: agent-driven ML job creation did not work in Alexander's testing
- [x] Streams
	  - [x] [Add unparsed log source to demo Streams partition, parsing, and significant events with AI](https://elastic.slack.com/archives/C0AKHB2QQNB/p1778079624113199) — include good "log values" so it can feed the dashboard skill demo above
- [x] Dashboard skill
	  - [x] [Demo dashboard creation via AI agent skills](https://elastic.slack.com/archives/C0AKHB2QQNB/p1778079624113199) — end-to-end flow: parse a stream, then ask the assistant to analyze the data and build a dashboard from it. Working prompt sequence (Alexander): "which visualisation can be built with the data in the index X" → "can you show me the visualisations" → "can you create all initially proposed visualisations on a new dashboard"
- [x] Use Elastic color theme
- [x] Use new [human in the loop capability](https://elastic.slack.com/archives/C08LX7YSHU2/p1776182977290509)
- [x] Delete old dashboard page
- [x] Delete old landing page
- [x] Improve Landing page
	  - [x] Keep it — both Peter and Alexander like it
	  - [x] Add ability to modify a demo in progress to match vertical industry type (Peter)
	  - [x] Delete old page still available at /home?deployment_id=space
- [x] Move Github repo
	  - [x] Move to github.com/elastic
- [x] Metrics
	  - [x] Every scenario should produce meaningful metrics to support a metrics story during the demo
	  - [x] Include TS metrics-*
	  - [x] PROMQL queries
- [x] Bump to 9.4.0 GA build
- [x] Agent
	  - [x] How to enable skills on 9.4.0-SNAPSHOT?
	  - [x] Add skills API info to @AGENTS.md
	  - [x] Add skills
	  - [x] [Enable Elastic capabilities](https://elastic.slack.com/archives/C08LX7YSHU2/p1775613043081299) setting on the agent
- [x] Streams
	  - [x] Create a Streams partition per scenario so each scenario's data is isolated in its own stream
	  - [x] Add streams processing




%% kanban:settings
```
{"kanban-plugin":"board","list-collapse":[false,false,false,true],"show-checkboxes":true,"full-list-lane-width":false,"move-tags":true}
```
%%