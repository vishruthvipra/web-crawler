Summary

Your seed urls i.e. urls in your first frontier
	http://www.basketball-reference.com/awards/nba_50_greatest.html
	http://www.basketball-reference.com/leaders/per_career.html

	http://en.wikipedia.org/wiki/Michael_Jordan
	http://www.biography.com/people/michael-jordan-9358066

Count of unique urls indexed individually
18693

Time taken to crawl
15 hours

Total disk space size of your crawl or ES index size if applicable for Individual crawl
2.79 GB

Time taken to merge
1 h 10 min

Count of unique urls in Merged Index
56379 

Merged ES index size
5.3gb

How do you decide which links to put in your Frontier list and which to ignore.
1. The outlinks are canonicalized and then the outlinks are checked if they belong to any of the keywords in the list of keywords.
2. Those which belong to the keywords are sent to the prioritizer.
3. In prioritizer, each outlink is given a priority of 1 (least)
4. The number of links pointing to this outlink is taken and added for each outlink in a dictionary (X)
5. Each outlink's priority is added to the length of values(X) thereby increasing the priority number
6. Those links with priority still 1 are discarded.

How do you decide which link to crawl next, from your Frontier list.
1. Each link is checked with a set (visited_links) and if it is not in visitedlinks, it is considered for crawling
2. In Crawling, each link's robots.txt file is checked to see if the link is allowed or disallowed to crawl and crawled if it is allowed
3. After knowing the link is allowed to crawl, the time delay is read for each link
4. A dictionary containing {link: last_time_visited} is accessed to see the last time a link was accessed
5. Difference = current_time - last_time_visited
6. Sleep for difference seconds
7. Start to crawl