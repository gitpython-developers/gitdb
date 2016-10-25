#########
Changelog
#########

2.1.0
* **BREAKING API:** retrofit streams and (internal) packers as context-managers.

  Specifically if you are using directly the packers 
  (``git.pack.PackIndexFile``, ``git.pack.PackFile`` & ``git.pack.PackEntity``) 
  they must always be used from within a ``with ..`` block, or else 
  you will get *mmap-cursor* missing errors.
    
  .. Tip::

    You can "enter" `PackIndexFile`` & ``PackFile`` multiple time, but ``PackEntity`` only once
    to detect and avoid sloppy deep-nesting.
    Since ``git.pack.PackEntity`` class just coalseces ``PackIndexFile`` & ``PackFile``,
    you may "enter" either each internal packer separately, or the entity only once.
         
* **BREAKING API:** some utilities moved between ``git.util``, ``git.const`` & ``git.utils.compat``.
* Fix (probably) all leaks in Windows.  

  .. Note::
  
    The problem is that on Linux, any open files go unoticed, or collected by GC.
    But on *Windows* (and specifically on PY3 where GC is not deterministic), 
    the underlying files cannot delete due to *access violation*.
    
    That's a Good-thing|copy|, because it is dangerous to *leak*  memory-mapped handles.
    Actually *Windows* may leak them even after process who created them have died, 
    needing a full restart(!) to clean them up (signing-out is not enough).
      

* Stop importing *on runtime* *smmap* submodule - deleted completely submodule from sources.

  .. Tip::
  
      Developer now has to specify specific dependency to *smmap* in ``requirements.txt`` file, and
      remember to updated it before a final release.

* Run TCs also on Appveyor.  
  
    
0.6.1
=====

* Fixed possibly critical error, see https://github.com/gitpython-developers/GitPython/issues/220

    - However, it only seems to occur on high-entropy data and didn't reoccour after the fix


0.6.0
=====

* Added support got python 3.X
* Removed all `async` dependencies and all `*_async` versions of methods with it.


0.5.4
=====
* Adjusted implementation to use the SlidingMemoryManager by default in python 2.6 for efficiency reasons. In Python 2.4, the StaticMemoryManager will be used instead.


0.5.3
=====
* Added support for smmap. SmartMMap allows resources to be managed and controlled. This brings the implementation closer to the way git handles memory maps, such that unused cached memory maps will automatically be freed once a resource limit is hit. The memory limit on 32 bit systems remains though as a sliding mmap implementation is not used for performance reasons. 


0.5.2
=====
* Improved performance of the c implementation, which now uses reverse-delta-aggregation to make a memory bound operation CPU bound.


0.5.1
=====
* Restored most basic python 2.4 compatibility, such that gitdb can be imported within python 2.4, pack access cannot work though. This at least allows Super-Projects to provide their own workarounds, or use everything but pack support.


0.5.0
=====
Initial Release


.. |copy|   unicode:: U+000A9 .. COPYRIGHT SIGN