SQLiteClientSyncProvider
------------------------

This is a synchronization provider for use with Microsoft ADO.NET Sync
Services 1.0 for Devices. It is based on a project of the same name by
[Jose Romaniello](https://sourceforge.net/p/sqliteclientsyn/).

For usage example, please see
[SyncComm](https://synccomm.codeplex.com/). You will have to convert
SQL Server CE code to use SQLite, but it is a pretty straightforward
thing to do.

Third-party dependencies
------------------------

WinMobile dependencies:
- http://blogs.clariusconsulting.net/kzu/how-to-create-and-run-compact-framework-unit-tests-with-vs2008-and-testdriven-net/

SQLiteSync.Synchronization
--------------------------

This is based on Mono
[System.Runtime.Serialization](https://github.com/mosa/Mono-Class-Libraries/tree/master/mcs/class/corlib/System.Runtime.Serialization)
code. Note that this is NOT a general-purpose serialization solution,
but it does work if the server side serializes some very simple
objects in the SyncAnchor.

Troubleshooting
---------------

* you get: "Unable get to type ..." and "An error message cannot be
  displayed because an optional resource assembly containing it cannot
  be found"
    * copy C:\Program
      Files\Microsoft.NET\SDK\CompactFramework\v3.5\WindowsCE\Diagnostics\NETCFv35.Messages.EN.cab
      to the device and install it, then run the tests again

License
-------

Public domain, unless noted otherwise.

--Artyom Shalkhakov, artyom DOT shalkhakov AT gmail DOT com
