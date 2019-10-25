# SqlBulkHelpers
Library for efficient and high performance bulk insert and update of data from C# applications.  This library leverages the power of the C# SqlBulkCopy classes, but augments these with the following key benefits:

1. Provides a simplified facade for interacting with the SqlBulkCopy API.
* The Performance improvements have been DRAMATIC!
2. Provides enhanced support for ORM based objects with the SqlBulkCopy API.
3. Provides support for Database Tables (and ORM objects/models) that utilize an Identity Id column as the Primary Key.
* Dynamically retrieves the new Identify key values and populates them back on the Objects provided after import.

The SqlBulkCopy API provides fantastic performance benefits, but retrieving the Identity values for Primary Keys that are auto-generated is not a default capability.  And as it turns out, this is not a trivial
task despite the significant benefits it provides for developers.  

Providing support for a table with an Identity Column as the Primary Key is the critical reason for developing this library.  There is alot of good information on Stack Overflow and other web resources that provide
various levels of help for this kind of functionality, but there are few (if any) fully developed solutions to really help others find an efficient way to do this end-to-end.

**So, that was the goal of this library, to provide and end-to-end solution to either a) leverage directly if it fits your use-cases, or b) use as a model to adapt to your use case! 
so I hope that it helps others on their projects as much as it has helped ours.**

TODO:  I'm still working on gathering links for other information (to share here) and will improve this documentation with additional sample code for implementaiton . . . I hope to get some time to improve this
documentation over the next few days/weeks.

```
  
MIT License

Copyright (c) 2019 - Brandon Bernard

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

```
