# Configuration

## Astra `config.yml` file

Astra is configured through the `yaml` configuration file `~/.astra/config.yml` containing the following keys:

| key             | value                                                                                    |
| --------------- | ---------------------------------------------------------------------------------------- |
| `folder_assets` | path of the folder where astra reads and writes data                                     |
| `gaia_db`       | path of the local gaia database used by astra to plate solve images (used when pointing) |

Under the assets folder, the following strucure is created:

```
assets
├── images        # where astra saves the images
├── log           # log-related files
├── observatory   # observatory config files
└── schedule      # schedule files
```

Within Python, the configuration can be accessed through the `astra.CONFIG` object.
