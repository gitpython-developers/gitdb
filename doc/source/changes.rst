#########
Changelog
#########

****
NEXT
****
* Added interface to allow transporting git data: **TransportDBMixin**
* Added interface to allow reference resolution: **RefParseMixin**
* Added implementation of git datbase with support for transportation and reference resolution: **RefGitDB**

*****
0.5.2
*****
* Improved performance of the c implementation, which now uses reverse-delta-aggregation to make a memory bound operation CPU bound.

*****
0.5.1
*****
* Restored most basic python 2.4 compatibility, such that gitdb can be imported within python 2.4, pack access cannot work though. This at least allows Super-Projects to provide their own workarounds, or use everything but pack support.

*****
0.5.0
*****
Initial Release
