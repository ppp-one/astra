Getting Started
===============

First run
-------------

During the initial run, you will be prompted to configure your observatory.

#. From command line, run:

   .. code-block:: bash

      astra

#. Follow the terminal instructions, e.g.:
   
   .. code-block:: bash
 
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
 
   ..  attention:: The configuration files will be created in the `observatory_config` directory. You must edit them with your observatory's  information before using Astra in normal operation. See: :doc:`observatory configuration files <user_guide/observatory_configuration>`.


Normal operation
-----------------

Once the initial setup is complete, you can run Astra.

#. Ensure that your ASCOM Alpaca devices or simulators are running on your network and properly configured.

#. Run the command from command line:

   .. warning:: After initial setup, running this command will park your telescope and close your dome if the weather conditions are unfavorable or your safety monitor indicates an unsafe status.

     .. code-block:: bash

        astra

#. After a few moments, open your web browser and navigate to the following URL http://localhost:8000, and you should see Astra's web interface.


.. |pic1| image:: _static/ui-summary-tab.png
   :width: 49%
   :alt: Astra summary tab


.. |pic2| image:: _static/ui-log-tab.png
   :width: 49%
   :alt: Astra log tab


.. |pic3| image:: _static/ui-weather-tab.png
   :width: 49%
   :alt: Astra weather tab


.. |pic4| image:: _static/ui-controls-tab.png
   :width: 49%
   :alt: Astra controls tab


|pic1| | |pic2|

|pic3| | |pic4|

.. figure:: _static/ui-schedule-editor.png
   :width: 80%
   :align: center
   :alt: Astra schedule editor
