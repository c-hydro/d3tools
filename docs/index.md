---
hide:
  - toc
---

<section class="d3-hero" aria-labelledby="d3-hero-title">
  <div class="d3-hero__mark">
    <img src="assets/branding/d3tools.svg" alt="D3tools package logo">
  </div>
  <div class="d3-hero__copy">
    <p class="d3-eyebrow">From observations to drought indicators</p>
    <h1 id="d3-hero-title">Reusable workflow infrastructure for operational drought monitoring</h1>
    <p>
      D3tools provides the shared workflow and Python components needed to acquire
      inputs, aggregate data, calculate drought indicators, and operate the chain
      consistently over time.
    </p>
    <div class="d3-hero__actions">
      <a class="md-button md-button--primary" href="workflow_orchestration/getting-started/">Get started</a>
      <a class="md-button" href="tools/">Explore the tools</a>
    </div>
  </div>
</section>

<section class="d3-paths" aria-label="Documentation sections">
  <a class="d3-card" href="workflow_orchestration/">
    <span class="d3-card__accent d3-card__accent--orange"></span>
    <h2>Workflow orchestration</h2>
    <p>Configure datasets and engines, run workflow sections, inspect logs, and follow a complete CHIRPS-to-SPI case study.</p>
  </a>
  <a class="d3-card" href="tools/">
    <span class="d3-card__accent d3-card__accent--blue"></span>
    <h2>Python tools</h2>
    <p>Use the shared components directly for time, space, datasets, cases, thumbnails, cleanup, and notifications.</p>
  </a>
  <a class="d3-card" href="reference/config/">
    <span class="d3-card__accent d3-card__accent--gold"></span>
    <h2>API reference</h2>
    <p>Consult signatures and members generated from the public Python API and its verified docstrings.</p>
  </a>
</section>

## Where D3tools fits

D3tools is the coordination layer for an operational monitoring chain. It provides the
shared configuration model, datasets, timestepping, logging, execution order, and other
reusable building blocks, but it does not itself download products, perform the domain
processing, or calculate drought indicators. Engine-backed workflows therefore require
the companion package responsible for each stage to be installed:

- **[DOOR](https://github.com/c-hydro/door/tree/main_new)** acquires source data from remote services
  and organizes it into the datasets consumed by later stages.
- **[DAM](https://github.com/c-hydro/dam)** processes those inputs and creates the
  aggregations and derived datasets needed for indicator calculation.
- **[DRYES](https://github.com/c-hydro/dryes)** calculates drought indicators from the
  prepared inputs and their corresponding parameter datasets.

A workflow can use only the engines needed for its purpose—for example, a download-only
workflow may use DOOR alone—but a complete operational drought-monitoring chain will
typically combine all three. D3tools loads their workflow sections, runs them in order,
and supplies the common infrastructure around them; each engine remains responsible for
the scientific and domain-specific behaviour within its section. The standalone Python
tools documented here can also be imported independently when no workflow engine is
required.

<aside class="d3-cima">
  <div>
    <p class="d3-eyebrow">Developed by</p>
    <h2>CIMA Research Foundation</h2>
    <p>International Centre for Environmental Monitoring.</p>
    <a href="https://www.cimafoundation.org/en/" target="_blank" rel="noopener">Visit CIMA Research Foundation</a>
  </div>
  <a class="d3-cima__logo" href="https://www.cimafoundation.org" target="_blank" rel="noopener" aria-label="Visit CIMA Research Foundation">
    <img src="assets/branding/CIMA_logo.svg" alt="CIMA Research Foundation">
  </a>
</aside>
