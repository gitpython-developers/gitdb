#########
Changelog
#########

****
NEXT
****
* == **General Design** ==

 * Database Interfaces have no implementation anymore (or only a minimal most general one where adequate). All previous implementation moved to the *py* subdirectory, which is designated to the pure python implementation. It

* == **Renamed Types** == 

 * Renamed type *GitDB* to **GitODB**. *GitDB* now is a new type with more functionality
 * Renamed type **FileDB** to **RootPathDB**
 * The previous implementations for all types found in db/base.py are now renamed to **Pure**<PreviousName> and moved to the db/py/base.py module. 
 
* == **Renamed Modules** == 

 * in *gitdb/db*
 
  * moved all modules *except for* base.py into **py/** subdirectory
  * renamed **base.py** into **interface.py**
  
* == **New Modules** ==

 * gitdb/db/py/base.py - pure python base implenentations for many simple interfaces which are subsequently used by complex pure implementations.


* == **New Interfaces** ==

 * Added interface to allow transporting git data: **TransportDB**
 * Added interface to handle git related paths: **RepositoryPathsMixin**
 * Added interface to read and write git-like configuration: **ConfigurationMixin**
 * Added **ReferencesMixin** providing reference resolution.
 * Added implementation of git datbase with support for transportation and reference resolution: **GitDB**

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
