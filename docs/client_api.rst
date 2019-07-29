Usage examples
==============

Basic information
-----------------

Get available entity types
::
   c = tdmq.Client()
   print(c.get_entity_categories())
   print(c.get_entity_types())   
   print(c.get_geometry_types())
   
Get sources
::
   sources = c.get_sources()
   src_by_type = dict((s.entity_type, s) for s in sources)
   src_by_type2 = dict((t, c.get_sources({'entity_type': t}))
                       for t in c.get_entity_types())

       

   
Footprint and geometry
----------------------

Find movable and stationary sources:
::
   c = tdqm.Client()
   sources = c.get_sources({
      'roi': 'rectangle((9.2215, 30.0015), (9.3, 30.01))'})
   moving_sources = 
   static_sources = [s for s in sources if s.stationary]
   for s in [_ for _ in sources if _.stationary]:
       print(f'Source {s.id}({s.tdmq_id}) is @ {s.default_footprint}')
   for s in [_ for _ in sources if not _.stationary]:
       ts = s.get_timeseries(fields=['geometry'])
       print(f'Source {s.id}({s.tdmq_id})')       
       for t, geometry in ts[:]:
           print(f'\t{ts.timebase} + {t} secs: @ {geometry}')

Timeseries
----------

Get temperature evolution as measured by dpc during last week:
::
   c = tdmq.Client()
   s = c.get_source({'id': 'dpc-temperature-mosaic'})
   print(s.default_footprint)
   before = timedate.now()
   after = before - timedelta(seconds=7*24*3600)
   ts = s.timeseries({'after': after, 'before': before})
   # show temperatures for the whole country
   (delta_t, temp) = ts[4]
   plt.imshow(temp)
   # show temperatures in Sardinia
   ts = s.timeseries({'after': after, 'before': before, 'roi': roi})
   for dt, temp in ts[:]:
       plt.imshow(temp)
   # get subregion
   for dt, temp in ts[:, 20:100, 24:80]:
       plt.imshow(temp)   

   # compare with other available information
   sources = c.get_sources({'after': after, 'before': before, 'roi': roi,
                            'controlledProperty': ['land_temperature']})

			    
   
   
   
       
