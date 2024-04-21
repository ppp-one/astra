# Schedule

## yaml format

Astra schedules can be defined as a yaml file located in `assets/schedule` with the following syntax:

```yaml
- {action_type}:
    # action parameters
    key0: value
    key1: value
    ...

    # time window
    start_time: value
    end_time: value
```

For example:

`Callisto.yaml`
```yaml
- open:
    start_time: '2024-01-11 23:31:40.915'
    end_time: '2024-01-12 10:07:40.253'

- flats:
    filter: ['I+z']
    n: [10]
    start_time: '2024-01-11 23:31:40.915'
    end_time: '2024-01-12 00:16:20.020'

- object:
    object: Sp0711-3824
    filter: I+z
    ra: 107.7545375
    dec: -38.41298694444444
    exptime: 13
    guiding: true
    pointing: false
    start_time: '2024-01-12 00:16:20.020'
    end_time: '2024-01-12 04:49:20.020'

- object:
    object: Sp0853-0329
    filter: I+z
    ra: 133.40066666666664
    dec: -3.4922780555555555
    exptime: 21
    guiding: true
    pointing: false
    start_time: '2024-01-12 04:51:20.020'
    end_time: '2024-01-12 09:23:00.030'

- flats:
    filter: 'I+z'
    n: 10
    start_time: '2024-01-12 09:23:00.030'
    end_time: '2024-01-12 10:07:40.253'

- close:
    start_time: '2024-01-12 10:07:40.253'
    end_time: '2024-01-12 10:12:40.253'
  
- calibration:
    exptime: [0,10,13,15,21,30,60,120]
    n: [10,10,10,10,10,10,10,10]
    start_time: '2024-01-12 10:12:40.253'
    end_time: '2024-01-12 10:37:40.253'
```

*TODO: describe each action and possible parameters*

## (original) CSV format
The original way of defining schedules in Astra is to use a csv file located in `assets/schedule` with the following syntax:

`Callito.csv`
```csv
device_type,device_name,action_type,action_value,start_time,end_time
Camera,camera_Callisto,open,{},2024-01-11 23:31:40.915,2024-01-12 10:07:40.253
Camera,camera_Callisto,flats,"{'filter': ['I+z'], 'n': [10]}",2024-01-11 23:31:40.915,2024-01-12 00:16:20.020
Camera,camera_Callisto,object,"{'object': 'Sp0711-3824', 'filter': 'I+z', 'ra': 107.7545375, 'dec': -38.41298694444444, 'exptime': 13, 'guiding': True, 'pointing': False}",2024-01-12 00:16:20.020,2024-01-12 04:49:20.020
Camera,camera_Callisto,object,"{'object': 'Sp0853-0329', 'filter': 'I+z', 'ra': 133.40066666666664, 'dec': -3.4922780555555555, 'exptime': 21, 'guiding': True, 'pointing': False}",2024-01-12 04:51:20.020,2024-01-12 09:23:00.030
Camera,camera_Callisto,flats,"{'filter': ['I+z'], 'n': [10]}",2024-01-12 09:23:00.030,2024-01-12 10:07:40.253
Camera,camera_Callisto,close,{},2024-01-12 10:07:40.253,2024-01-12 10:12:40.253
Camera,camera_Callisto,calibration,"{'exptime': [0, 10, 13, 15, 21, 30, 60, 120], 'n': [10, 10, 10, 10, 10, 10, 10, 10]}",2024-01-12 10:12:40.253,2024-01-12 10:37:40.253
```

```{warning}
This may be deprecated in a future version.
```