# Quickstart

## First run

During the initial run, you will be prompted to configure your observatory.

### 1. From command line, run:

   If using *uv*:
   ```bash
   uv run astra
   ```

   Or, if using *conda* or another activated Python environment:
   ```bash
   astra
   ```

### 2. Follow the terminal instructions, e.g.:

   ```{code-block} text
   :class: terminal-transcript
   :emphasize-lines: 7,17,24,54,56,65

   ============================================================
                  Welcome to Astra Configuration               
   ============================================================

   Please provide the following information to set up your observatory.

   Use default assets path (/Users/peter/Documents/Astra)? [y/n]: y

   ============================================================
                  Gaia Database Configuration                 
   ============================================================

   The Gaia-2MASS catalog enables offline plate solving and
   autofocus field selection. Choose a magnitude cut based on
   your needs (higher = more stars, larger file).

   Use Gaia database? [y/n]: y

   Options:
   1. Download Gaia database now (choose magnitude cut)
   2. I already have it (enter path)
   3. Skip for now (can add later in config)

   Select option [1/2/3]: 1

   ============================================================
               Select Gaia Database Magnitude Cut             
   ============================================================


   Magnitude Cut | Stars         | File Size
   ------------------------------------------------------------
          1      |           144 |   766.0 kB
          2      |           650 |   766.0 kB
          3      |            2K |   766.0 kB
          4      |            6K |   766.0 kB
          5      |           18K |     2.0 MB
          6      |           58K |     4.5 MB
          7      |          160K |    10.8 MB
          8      |          426K |    26.8 MB
          9      |            1M |    67.2 MB
         10      |            3M |   172.3 MB
         11      |            7M |   425.2 MB
         12      |           16M |   987.6 MB
         13      |           36M |     2.2 GB
         14      |           79M |     4.8 GB
         15      |          161M |     9.8 GB
         16      |          297M |    18.1 GB

   Recommendation: Magnitude 16 for most coverage
                   Magnitude 10-15 for most small-medium setups
                   Magnitude 1-9 for testing

   Select magnitude cut [1-16] or 'c' to cancel: 6

   Download filepath [/Users/peter/gaia_tmass_6_jm_cut.db]: 

   Downloading magnitude 6 database (4.5 MB)...
   URL: https://zenodo.org/records/18214672/files/gaia_tmass_6_jm_cut.db?download=1
   Destination: /Users/peter/gaia_tmass_6_jm_cut.db

   [████████████████████████████████████████] 100.0% (4.3 MB / 4.3 MB)     
   ✓ Download complete: /Users/peter/gaia_tmass_6_jm_cut.db

   Please enter the name of the observatory: ELT
   ✓ Configuration file created successfully.

   Warning: Observatory config files have not been modified from default templates.

   Please update your observatory configuration files located in:
   /Users/peter/Documents/Astra/observatory_config

   Unchanged files: ELT_fits_header_config.csv, ELT_config.yml

   Exiting until observatory configuration is updated.
   ```

   ```{important}
   The configuration files will be created in the `observatory_config` directory.
   You must edit them with your observatory's information before using *Astra* in normal
   operation.
   See: [observatory configuration files](user_guide/observatory_configuration).
   ```

## Normal operation

Once the initial setup is complete, you can run *Astra*.

1. Ensure that your ASCOM Alpaca devices or simulators are running on your network and properly configured.

2. Run the `astra` command from command line again (as above):

   ```{warning}
   After initial setup, running this command will park your telescope and close
   your dome if the weather conditions are unfavorable or your safety monitor
   indicates an unsafe status.
   ```

3. After a few moments, open your web browser and navigate to the URL `http://host:port`,
  [http://localhost:8000](http://localhost:8000) by default, and you should see 
  Astra's web interface.

<img alt="Astra summary tab" src="_static/ui-summary-tab.png" style="width: 49%; margin: 5px 0;" />
<img alt="Astra log tab" src="_static/ui-log-tab.png" style="width: 49%; margin: 5px 0;" />

<img alt="Astra weather tab" src="_static/ui-weather-tab.png" style="width: 49%; margin: 5px 0;" />
<img alt="Astra controls tab" src="_static/ui-controls-tab.png" style="width: 49%; margin: 5px 0;" />


<div style="text-align: center;">
   <img alt="Astra schedule editor" src="_static/ui-schedule-editor.png" style="width: 80%; align=center; margin: 5px 0;" />
   <img alt="Astra fits viewer" src="_static/ui-fits-viewer.jpg" style="width: 80%; align=center; margin: 5px 0;" />
</div>
