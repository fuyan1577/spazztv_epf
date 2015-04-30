# spazztv_epf
Multi-threaded Data Loader for iTunes Enterprise Partner Feed

After many requests from friends and collegues, we've finally made this public.

I created spazztv_epf a couple of years back as an optimized java port of Apple's EPFImporter.py that imports thew
EPF (Enterprise Partner Feed) Data into a database. This version includes the following features:

* Multi-threaded Design - 1 thread for each table import/update
* JDBC Connection Pool using Oracle's UCP
* Data Writer Interface - includes MySQL & Oracle DB Implementations
* Supports Config File & Command Line configuration

FYI: The spazztv_epf project is the specific project that replaces EPFImporter.py.

-Tom Billingsley
