# Quickstart

## First run

During the initial run, you will be prompted to configure your observatory.

1. From command line, run:

   ```bash
   astra
   ```

2. Follow the terminal instructions, e.g.:

   ```text
   Welcome to Astra! Please provide the following information:

   Use default assets path (/Users/peter/Documents/Astra)? [y/n]: y

   Use local Gaia DB? [y/n]: y
   Please enter the path to Gaia DB: /Users/peter/gaia_tmass_16_jm_cut.db

   Please enter the name of the observatory: ELT

   Created config file.
   Created folder /Users/peter/Documents/Astra
   Created folder /Users/peter/Documents/Astra/observatory_config
   Created folder /Users/peter/Documents/Astra/schedules
   Created folder /Users/peter/Documents/Astra/logs
   Created folder /Users/peter/Documents/Astra/images

   Warning: Observatory config files have not been modified from default templates. Please update the following files with your  observatory's information in:

   /Users/peter/Documents/Astra/observatory_config
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

2. Run the command from command line:

   ```{warning}
   After initial setup, running this command will park your telescope and close
   your dome if the weather conditions are unfavorable or your safety monitor
   indicates an unsafe status.
      ~~~bash
      astra
      ~~~
   ```

3. After a few moments, open your web browser and navigate to the URL `http://host:port`,
  [http://localhost:8000](http://localhost:8000) by default, and you should see 
  Astra's web interface.

<img alt="Astra summary tab" src="_images/ui-summary-tab.png" style="width: 49%; margin: 5px 0;" />
<img alt="Astra log tab" src="_images/ui-log-tab.png" style="width: 49%; margin: 5px 0;" />

<img alt="Astra weather tab" src="_images/ui-weather-tab.png" style="width: 49%; margin: 5px 0;" />
<img alt="Astra controls tab" src="_images/ui-controls-tab.png" style="width: 49%; margin: 5px 0;" />


<div style="text-align: center;">
   <img alt="Astra schedule editor" src="_images/ui-schedule-editor.png" style="width: 80%; align=center margin: 5px 0;" />
</div>