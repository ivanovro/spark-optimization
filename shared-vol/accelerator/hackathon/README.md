# Spark Hackathon

During this hackathon you're going to use Spark to analyze the logs of a major Dutch news website. These logs contain all 
kinds of interesting information that can be used to gain insight into what people are interested in. Our final goal is to find "trending" 
news topics, that were interesting during a given period.

## Prerequisites:

 - Python 3 environment;
 - Your favourite IDE;
 - A working Spark installation. Possibilities include:

    - On macOS, the Homebrew installation: `brew install apache-spark`
    - Via pip (in a virtualenv): `mkvirtualenv hackathon && pip install pyspark`
    - Manual installation; see [Spark Downloads][1].

[1]: https://spark.apache.org/downloads.html

## Outline

During the hackathon, you will:

 - Parse HTTP log data into a usable format with Spark.
 - Determine the most popular news categories per time window.
 - Categorize the logs into device type categories.
 - Apply windowing logic to track usage over user sessions.
 - Report the most popular device type per (configurable) time window.
 - Figure out if any of the news categories are particularly popular (aka trending) during a given period.
 - Write tests to ensure the beautiful code you wrote works as designed/expected.
 - _Bonus:_ Apart from windowing over time, these logs lend themselves to session-based windowing…

## The Data

Data for this hackathon can be found in the `data/` directory. Before you start, you may need to run a script to fetch larger datasets:

    % ./data/fetch-data

There are 2 versions of the log data that you can work with in this exercise:
We'll provide you with 2 versions of the data.

1. A small, human readable sample that you can use to get an understanding of what the data looks like.

   This set can be found here: `data/apachelog_small/`

2. A much larger set of Parquet files that contains the dataset your code should actually run on.

   This set can be found here: `data/apachelog_large/`

### Step 1: Extract the page name per log line

Parse the logs into a Spark Dataframe. Your schema should look like this:
```
root
 |-- ip_address: string (nullable = true)
 |-- timestamp: timestamp (nullable = true)
 |-- url: string (nullable = true)
 |-- status_code: integer (nullable = true)
 |-- response_bytes_size: integer (nullable = true)
 |-- user_agent: string (nullable = true)
```

### Step 2: Show the top _N_ most popular news categories overall

We'd like to get some insight into what topics are popular. Extract the news category from the logs, and show the top _N_ most common categories over the entire dataset.

For example the link `https://www.nu.nl/opmerkelijk/5699843/japan-pakt-israeliers-4-ton-goud-smokkelden-in-auto-onderdelen.html` has a category of `opmerkelijk`.

### Step 3: Show the top _N_ most popular news categories per (configurable) time window

Although the previous exercise gives us some insight into popularity of news topics, it's still rather limited. Instead of the popularity over all time, compute the top _N_ per time window.

### Step 4: Enrich the logs with the device type

Add information to your table indicating what type of device generated this log line. We identify 3 categories:

1. PC
2. Tablet
3. Phone

We've made a mapping of user agent string to device type for you. You can find it here: `data/user_agents_map.csv`

### Step 5: Compute the top _N_ most popular news categories per device type per time window

To make our analysis even more complete: are there any particular differences visible if we use this device type to distinguish users? Compute the top _N_ most popular news categories per device type. Do this on both the full dataset and on the configurable windows.

### Step 6: Are any of the news categories trending in any way? 

_Trending_ has many definitions. Using our current definition some topics would always be trending: you can imagine the homepage will always get more hits than any other news category, but that doesn't necessarily mean it's trending. We'd like to do something smarter. Instead of the absolute counts, compute the slopes over the windows using these counts.

### Step 7: Session clicks

User sessions are a commonly used definition when dealing with web logs. Here, we define a session as:

> All clicks made by a single user within 1 hour. Clicks from a given IP address with the same user agent string are assumed to be the same user.

Show the top _N_ sessions based on number of clicks.

### Step 8: Add unit tests for your application

Refactor your application and implement unit tests to ensure it's doing the right thing.

### Step 9 (optional): Apply session-based windowing

Windowing over time is just one way of looking at time series data. Try windowing over sessions instead. Can you find different patterns?
