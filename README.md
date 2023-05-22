# Astra - Automated Survey observaTory Robotised with Alpaca

Astra is an open-source observatory control software designed for automating and managing the operations of a survey observatory. It is built to work seamlessly with ASCOM Alpaca.

## Features

- **Remote Control**: Astra enables remote control of observatory equipment, allowing you to operate the observatory from anywhere in your local network, or anywhere with an internet connection when using a VPN or a tunneling service such as ngrok.
- **Automated Surveys**: You can schedule and execute automated survey observations using Astra. Define observation targets, parameters, start and end times, and let Astra handle the rest.
- **Alpaca Integration**: Astra leverages the Alpyca python library to provide seamless integration with a wide range of astronomy equipment, permitting easy scalability to multiple telescopes, cameras, domes etc.

![Alt text](astra-art.png)

## Getting Started

(TODO: video tutorial)

### Installation

```
git clone ...
cd astra
conda env create -f environment.yml
cd code
```

### Usage

Before starting Astra:

1. Have Alpaca compliant equipment or simulators active in your local network. 
2. Edit the config file `config/<observatory-name>.yml` to specify the observatory parameters and equipment's connection with Alpaca. See `config/Io.yml` as an example.
3. Have a schedule file `schedule/<observatory-name>.csv` ready to be used. See `schedules/Io.yml` as an example.
4. Then run the following commands to start Astra:

```
conda activate astra
cd src
uvicorn main:app --reload --port 8000
```

5. Open the browser and go to `http://localhost:8000/` to access Astra.

## Contributing

Contributions are welcome and appreciated! If you want to contribute to Astra, please follow the guidelines outlined in [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Astra is released under the GNU General Public License v3.0. See [LICENSE](LICENSE) for more details. In short, this means that you are free to use, modify, and distribute Astra as long as you make your modifications available under the same license.

## Contact

If you have any questions, suggestions, or feedback, please leave an issue on the GitHub repository.