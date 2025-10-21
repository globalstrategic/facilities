# Facilities Documentation Index

**Quick Navigation**: Start here to find the right documentation for your needs.

---

## 📚 Primary Documentation

### [README_FACILITIES.md](README_FACILITIES.md)
**User Guide & API Reference** - Start here for using the facilities database

- Data model and JSON schema
- Import workflows and examples
- Entity resolution (countries, metals, companies)
- Deduplication system
- Query patterns and examples
- Data quality guidelines

**Best for**: New users, import workflows, querying facilities, deduplication

---

### [../CLAUDE.md](../CLAUDE.md)
**Developer Guide** - For developers working with the codebase via Claude Code

- Code architecture and patterns
- Development commands (import, dedup, enrich)
- Duplicate detection strategy (4-priority system)
- Testing patterns
- Common workflows (adding countries, debugging, deduplication)
- Scripts reference (all production scripts)
- Performance characteristics

**Best for**: Development, architecture understanding, workflow automation, troubleshooting

---

## 🏗️ Architecture & Integration

### [ENTITYIDENTITY_INTEGRATION_PLAN.md](ENTITYIDENTITY_INTEGRATION_PLAN.md)
**EntityIdentity Integration Architecture** (920 lines)

- Complete integration design
- Company resolution with quality gates
- Metal normalization with chemical formulas
- Country auto-detection
- Facility matching strategies
- Two-phase company resolution pattern
- Testing framework

**Best for**: Understanding entity resolution system, integration architecture

---

### [SCHEMA_CHANGES_V2.md](SCHEMA_CHANGES_V2.md)
**Schema v2.0.0 Documentation** (453 lines)

- Schema evolution from v1 to v2
- New fields: `chemical_formula`, `category`, `company_mentions[]`
- Phase 2 company resolution fields
- Migration guide
- Backward compatibility notes

**Best for**: Schema reference, migration planning, field definitions

---

## 🔄 Specialized Workflows

### [DEEP_RESEARCH_WORKFLOW.md](DEEP_RESEARCH_WORKFLOW.md)
**Gemini Deep Research Integration** (396 lines)

- Research prompt generation
- Processing research outputs
- Company resolution during enrichment
- Batch processing workflows
- Output formats

**Best for**: Enriching facilities with AI research, batch updates

---

### [guides/RESOLUTION_WORKFLOW.md](guides/RESOLUTION_WORKFLOW.md)
**Entity Resolution Workflow** (~11KB)

- Detailed resolution workflows
- Country, metal, and company resolution
- Confidence scoring
- Quality gates
- Edge cases and troubleshooting

**Best for**: Deep dive into resolution logic, quality gate tuning

---

## 📖 Historical Documentation

### [implementation_history/](implementation_history/)

Historical design decisions, migration plans, and point-in-time analyses:

- `COVERAGE_2025-10-20.md` - Test coverage snapshot
- `PHASE_2_ALTERNATE_PATH.md` - Company resolution design decision

**Best for**: Understanding design decisions, historical context

---

## 📝 Version History

### [../CHANGELOG.md](../CHANGELOG.md)
**Complete version history and release notes**

- Version 2.0.1 (2025-10-21): Deduplication system
- Version 2.0.0 (2025-10-20): EntityIdentity integration
- Earlier versions and migration notes
- Known issues and deprecations

**Best for**: What's new, migration guides, version compatibility

---

## 🗺️ Documentation Map by Use Case

### I want to...

**Import facilities from a CSV/report:**
→ [README_FACILITIES.md - Workflows](README_FACILITIES.md#workflows)

**Clean up duplicate facilities:**
→ [README_FACILITIES.md - Deduplication](README_FACILITIES.md#5-deduplication)
→ [CLAUDE.md - Deduplicating Facilities](../CLAUDE.md#deduplicating-facilities)

**Understand the code architecture:**
→ [CLAUDE.md - Code Architecture](../CLAUDE.md#code-architecture)

**Resolve company names to canonical IDs:**
→ [README_FACILITIES.md - Company Resolution](README_FACILITIES.md#3-company-resolution)
→ [ENTITYIDENTITY_INTEGRATION_PLAN.md](ENTITYIDENTITY_INTEGRATION_PLAN.md)

**Understand the JSON schema:**
→ [README_FACILITIES.md - Data Model](README_FACILITIES.md#data-model)
→ [SCHEMA_CHANGES_V2.md](SCHEMA_CHANGES_V2.md)

**Run tests:**
→ [CLAUDE.md - Testing Patterns](../CLAUDE.md#7-testing-patterns)

**Enrich facilities with research:**
→ [DEEP_RESEARCH_WORKFLOW.md](DEEP_RESEARCH_WORKFLOW.md)

**Contribute to the codebase:**
→ [CLAUDE.md](../CLAUDE.md)
→ [ENTITYIDENTITY_INTEGRATION_PLAN.md](ENTITYIDENTITY_INTEGRATION_PLAN.md)

**Check what's new:**
→ [CHANGELOG.md](../CHANGELOG.md)

---

## 📊 Quick Stats

**Current Database** (2025-10-21):
- ~8,455 facilities across 129 countries
- 99.3% with coordinates
- 50+ metals/commodities
- Schema v2.0.1

**Documentation Files**: 9 active documents
- 2 primary guides (README_FACILITIES.md, CLAUDE.md)
- 4 specialized references
- 2 historical docs
- 1 changelog

---

## 🔗 External Links

- [EntityIdentity Library](https://github.com/globalstrategic/entityidentity) - Entity resolution backbone
- [Gemini Deep Research](https://deepmind.google/technologies/gemini/) - AI research integration
- [JSON Schema Specification](https://json-schema.org/) - Schema validation reference

---

## 📞 Support

For questions or issues:
1. Check this INDEX for relevant documentation
2. Review [CHANGELOG.md](../CHANGELOG.md) for recent changes
3. Check [CLAUDE.md - Known Issues](../CLAUDE.md#known-issues--gotchas)
4. Review import logs in `output/import_logs/`

---

**Last Updated**: 2025-10-21
