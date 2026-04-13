# Held-Out Set

Record held-out evaluation queries and expected answer notes here.

## Template

```md
## Query ID
Dataset:
Question:
Expected answer notes:
Why this query matters:
```

## yelp-q2
Dataset: yelp
Question: Which U.S. state has the highest number of reviews, and what is the average rating of businesses in that state?
Expected answer notes: State should resolve to PA/Pennsylvania; numeric value should match validator tolerance near 3.6994.
Why this query matters: Exercises cross-source mapping and aggregation semantics.

## yelp-q3
Dataset: yelp
Question: During 2018, how many businesses that received reviews offered either business parking or bike parking?
Expected answer notes: Count should resolve to 35 in current verified path.
Why this query matters: Exercises attribute parsing and year extraction under mixed date formats.

## yelp-q6
Dataset: yelp
Question: Which business received the highest average rating between January 1, 2016 and June 30, 2016, and what category does it belong to? Consider only businesses with at least 5 reviews.
Expected answer notes: Business should resolve to Coffee House Too Cafe with categories present.
Why this query matters: Exercises date-window filtering plus category extraction quality.
