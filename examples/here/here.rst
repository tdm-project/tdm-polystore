Here data ingestor
==================


The `ingestor.py` script is an example of a tool that will download
data from HERE and store it appropriately in TDMQ.


Traffic flow data
-----------------

HERE provides a service that will deliver traffic flow information by
providing for each relevant **road segment** the following data:

CF (Current Flow)
    This element contains details about speed and Jam Factor
    information for the given flow item.
CN (Confidence)
    an indication of how the speed was determined. -1.0 road
    closed. 1.0=100% 0.7-100% Historical Usually a value between .7
    and 1.0
FF (Free Flow)
    The free flow speed on this stretch of road.
JF (Jam Factor, Quality of Travel)
   The number between 0.0 and 10.0 indicating the expected quality of
   travel. When there is a road closure, the Jam Factor will be 10. As
   the number approaches 10.0 the quality of travel is getting
   worse. -1.0 indicates that a Jam Factor could not be calculated
SP (Speed)
   Speed (km/h) capped by speed limit
SU (Speed uncapped)
   Speed (km/h) not capped by speed limit

As an example:
::
   {'SP': 19.0,
    'SU': 19.0,
    'FF': 20.0,
    'JF': 0.0,
    'CN': 0.7}


Road segments are identified by two TMC points. For instance:
::
   {'TMC': {'PC': 7682,
	     'DE': 'Casa Cantoniera Di Marrubiu - SS131',
	     'QD': '+',
	     'LE': 0.34408}
   {'TMC': {'PC': 7681,
	    'DE': 'Marrubiu',
	    'QD': '+',
	    'LE': 3.3839}

HERE provides also SHP infomation (a polyline) on the road segment
that connects the first TMC to the following one.  In some special
cases, a segment can be identified by a single TMC (plus SHP data)

The traffic flow data is processed as follows:

 1. the flow data provided by HERE is reorganized as flow data by segment;
 2. for each segment:
    1. fetch src for that segment, if does not exist, create it
    2. ingest the flow data for that segment.




HERE documentation extracts
---------------------------

The following url provides access to xsd files that define the return data.

https://developer.here.com/documentation/traffic/topics/additional-parameters.html


FunctionalClassType

https://developer.here.com/documentation/routing/topics/resource-type-functional-class.html


The functional class is used to classify roads depending on the speed,
importance and connectivity of the road.  The value represents one of
the five levels:

 1. allowing for high volume, maximum speed traffic movement
 2. allowing for high volume, high speed traffic movement
 3. providing a high volume of traffic movement
 4. providing for a high volume of traffic movement at moderate speeds
    between neighbourhoods
 5. roads whose volume and traffic movement are below the level of any
    functional class


    
