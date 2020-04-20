<h1>SimpleDB Lab 1实验报告</h1>
<h3>1.整体框架</h3>
​		Lab1有6个练习，需要修改的类有TupleDesc.java、Tuple.java、Catalog.java、Bufferpool.java、HeapFile.java、HeapPage.java、HeapPageId.java、RecordId.java、SeqScan.java。它要完成的工作是实现元组、模式、存放模式（每一张表）的数据字典、存储元组的页、存储页的文件，完成读取元组、读取模式、通过缓冲池存储和获取页、遍历文件中存放的所有元组的功能。其中，TupleDesc、Catalog、Tuple是逻辑层面的模式设计，BufferPool、HeapFile、HeapPage是物理层面的模式设计。

​		实际上，Lab1是在实现支持完成SELECT * FROM TableName这个SQL语句的所有数据库搭建。数据项如何进行存储和表示由Tuple.java实现，RecordId.java是将各数据项形成的记录进行唯一性标识，以更好地存储在各自所在的数据块中；关系表的模式设计由TupleDesc.java实现；当我们有很多张表时通过Catalog.java进行组织和存放，相当于数据字典；而数据项组成记录之后所存储的地方——数据块由HeapPage.java实现，HeapPageId.java是用来标识每一个数据块，从而更好地构建存储架构；数据块需要存储到文件中，文件由HeapFile.java实现；BufferPool.java是一个缓冲池，模拟底层的主存储模式；SeqScan.java实现了正确地将外存的数据分批次读入内存以遍历的功能。

![未命名文件](C:\Users\麦子\Desktop\未命名文件.png)



<h3>2.设计思路</h3>
<h6>2.1 Tuple.java</h6>
​		对于一个元组，它需要的数据成员有TupleDesc schema，RecordId recordId，以及该元组包括的所有数据项。考虑到对一个元组的单个数据项更多的操作是修改和查询，因此将数据项列表设置成ArrayList的类型，数组类型会比链表类型效率高。



<h6>2.2 TupleDesc.java</h6>
​		该类是关系数据库模型中的一个关系表的模式。考虑到数据库模式在完成数据库开发之后很少在进行插入和删除的修改，反而是获取模式中的字段名字和字段类型的情况比较多，因此存储一个数据项TDItem的字段信息时，依然采用ArrayList的类型。

​		其中值得注意的是，本类中有一个getSize()的函数，是获取一个表中所有属性的存储大小的，也就是可以知道一个记录的字节大小，方便后续的存储和读取。这个函数在最开始实现的时候功能不明显，但在完成数据底层存储的时候就能体现其作用。

​		另外，在该类中我没有实现hashCode()函数，主要是还没有明白它的作用，后续也没有将TupleDesc作为HashMap的key值。我猜想在生成每一个表自己的Id的时候，为了解决哈希索引可能出现因数据聚集而查找效率降低的问题。



<h6>2.3 Catalog.java</h6>
​		在本类中，我设计了一个Table类，用于将一个关系表的Id值、表名、该表的物理文件、表的主键的所有属性封装起来。而Catalog类的成员数据就是存放所有数据表的ArrayList。在设计之初，我用的是LinkedList类型，考虑到这个表目录更常用的是插入和删除，但在实现后续的类时发现，Catalog类中最频繁被调用的函数都跟查询目录中的表有关，因此改成ArrayList。

​		在该类中有一个通过表的Id值获取存储表中数据的文件、该表的模式等函数，在后续的类实现中有很大的作用，这在我做exercise2时是不理解并且没有留意到的。



<h6>2.4 BufferPool.java</h6>
​		这个缓冲池类我只需要实现getPage()函数。针对这个函数，目前来看函数内部是没有用到TransactionId tid和Permissions pid的。在实现的过程中，遇到最大的问题是不知道从哪里获得不在缓冲池的newPage，即数据块。直到实现完HeapFile.java的readPage()才意识到它的作用。这时候又出现新的问题，哪里可以有HeapFile的实例允许我调用readPage()这个函数呢？在观察HeapPage.java的构造函数中可以发现，要获取当前数据块存储的数据的模式，是通过Database.getCatalog().getTupleDesc(id.getTableId())来实现的。因此这说明，整个数据库只有一个Catalog，在数据库Database类创建实例之后随之创建，而这个Catalog就存储了所有数据库中的表。以此获得我们需要的HeapFile实例，在调用它的readPage()函数，就可以通过PageId得到我们需要的但不在缓冲池里的newPage。

​		在插入newPage之前，我需要确认缓冲池中每有目标Page。一开始我不假思索地将缓冲池中存放Page的类型设置成LinkedList，主要是方便插入和删除。在搜索需要获得的目标Page时，就必须实现的遍历整个缓冲池，将目标Page的PageId和缓冲池中的Page的Id值进行比较，从而获取到需要的Page。但这个效率非常差。而Java提供了HashMap这种类型，可以以PageId作为Page的键值，从而在缓冲池中数据块数较大时可以大大提高了查找的效率。

![1584696128118](C:\Users\麦子\AppData\Roaming\Typora\typora-user-images\1584696128118.png)



<h6>2.5 HeapPage.java & HeapPageId.java</h6>
​		HeapPageId.java中的数据属性pgNo是指当前的Page是所在HeapFile（Table）的第几页，这是一个重要的数据属性，容易在实现HeapPageId.java时忽略。

​		HeapPage.java的理解重点在计算一个数据块中存储的记录数和标记记录的头指针。尽管lab1的markdown文件给出了计算公式，但我们仍然要清楚其中的原理。

​		对于获取记录的数目，我们的公式是floor((BufferPool.getPageSize()×8)/(td.getSize()×8+1))。一个数据块的字节大小是固定的，通过BufferPool.getPageSize()获得，但字节需要转换成bit，因此还需要×8，而对于一个记录而言，它不仅有自身的大小，还有1位的头指针，因此一个记录大小为tupleSize×8+1。在simpleDB我们规定了记录大小是定长的，每一个不同的记录大小可以通过TupleDesc的getSize()函数获得。由于数据块中不存储不完整的记录，因此使用floor()。

​		对于头指针，我需要知道它用了多少个8位的二进制数表示记录数。值得注意的是，头指针必须和记录数一一对应，因此当记录数无法整除8时，我们宁愿多浪费一些无用的位数，也必须做到一一对应，因此使用ceil()函数。这里也为后续处理头指针做了一个铺垫。

​		HeapPage.java的理解难点和实现难点在对头指针作用的理解以及根据头指针判断其中记录的有效性。每个头指针会以1代表有效，0代表无效去记录每一个存在数据块中的记录。但有效性记录只需要1个位，而要求又将头指针8位8位的组合在一起。因此在这里需要通过位运算去得到记录的有效与否值。

​		判断该记录是否有效，首先做的是将要查找的记录索引整除8，得到它所在的头指针组；其次，该索引还需要对8取余，从而知道在所在头指针组，记录对应的是第几个位置的有效值；最后进行先移位再与1进行和运算，得到有效与否的结果。

![1584698252575](C:\Users\麦子\AppData\Roaming\Typora\typora-user-images\1584698252575.png)

​		通过这个函数就可以获取该数据块仍可以存放记录的数目，以及筛选出真正有价值的记录，进行进一步处理。



<h6>2.6 HeapFile.java</h6>
​		本类的设计难点一个是理解numPages()函数应该如被实现，一个是iterator()函数的返回值DBFileIterator应该进行怎样合理的构造，还有一个是readPage()的函数实现。

​		numPages()函数是想要获取一个文件里存储了多少个数据块。由于数据块的大小是固定的，那么只需要知道文件的大小，就可以用文件大小/一个数据块大小得到数据块的数量。这个函数对我而言不容易理解关键在我对BufferPool类的功能及其数据没有深入地理解，导致在完成了很多代码后早已忽略了该类中还有pageSize这个重要数据属性。

​		而iterator()函数中需要返回一个自己设计的实现接口DBFileIterator的类，命名为HeapFileIterator。这个类的设计好坏直接影响到SeqScan.java的实现以及后期的调试。我在这个类的设计上吃了亏，出现的问题在3中再讲解。在设计这个类的时候，我的一开始的想法是，将所有当前文件中所有的Page都读取出来，存入一个链表中，作为构造HeapFileIterator的传入参数。然后在HeapFileIterator被打开，即调用open()函数的时候，第一个Page的Iterator会赋值给这个类的Iterator属性中。当第一个Page的Iterator被遍历完之后，就会将第二个Page的Iterator再赋值给类的Iterator属性中，通过pagePosition进行控制，以此类推。这种实现过程是将从缓冲池中读取文件的操作在构造Iterator之前就已经完成了。我觉得它的好处在于频繁的从缓冲池读取Page的操作被一次性先完成，通过空间换取时间的效率，因为我不清楚缓冲池实际的操作是否会涉及到磁盘的读取、内外存的交互。

```java
class HeapFileIterator implements DbFileIterator {
    private List<HeapPage> pages;
    private Iterator<Tuple> tupleIterator = null;
    private int pagePosition = 0;

    private HeapFileIterator(List<HeapPage> pages){
        this.pages = pages;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException, IOException, NoSuchFieldException {
        tupleIterator = pages.get(0).iterator();
        pagePosition = 0;
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException, NoSuchFieldException, IOException {
        if (pages == null) return false;
        if (tupleIterator == null) return false;
        if (!tupleIterator.hasNext() && pagePosition >= pages.size() - 1) return false;
        if (!tupleIterator.hasNext()) {
            tupleIterator = pages.get(++pagePosition).iterator();
        }
        return tupleIterator.hasNext();

    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException, NoSuchFieldException, IOException {
        if (!hasNext()) throw new NoSuchElementException();
        return tupleIterator.next();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException, IOException, NoSuchFieldException {
        close();
        open();
    }

    @Override
    public void close() {
        tupleIterator = null;
        pagePosition = 0;
    }
}
```

​		如果缓冲池只是内存开辟的空间，不需要在HeapFileIterator刚打开时就加载所有的文件，那么就可以将从缓冲池读取Page的操作放到HeapFileIterator内部去完成。思路是如果当前Page仍有未遍历完的Tuple，那就正常遍历；否则，当pagePosition比numPages()小，即当前Page的页码的下一个页码还小于文件的总数据块数，那就从缓冲池中读取下一个Page。

```java
private class HeapFileIterator implements DbFileIterator {
    private int pagePosition = 0;
    private Iterator<Tuple> tupleIterator;
    private TransactionId tid;

    HeapFileIterator(TransactionId tid) {
        this.tid = tid;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException, IOException, NoSuchFieldException {
        HeapPageId heapPageId = new HeapPageId(getId(), pagePosition);
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_ONLY);
        tupleIterator = heapPage.iterator();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException, NoSuchFieldException, IOException {
        if (tupleIterator == null) {
            return false;
        }
        if (tupleIterator.hasNext()) {
            return true;
        }
        if(pagePosition < numPages() - 1) {
            HeapPageId heapPageId = new HeapPageId(getId(), ++pagePosition);
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_ONLY);
            tupleIterator = heapPage.iterator();
            return tupleIterator.hasNext();
        } else {
            return false;
        }
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException, NoSuchFieldException, IOException {
        if (!hasNext()) throw new NoSuchElementException();
        return tupleIterator.next();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException, IOException, NoSuchFieldException {
        close();
        open();
    }

    @Override
    public void close() {
        tupleIterator = null;
        pagePosition = 0;
    }
}
```

​		针对readPage()函数的实现，主要是要注意一个小细节。要读的Page不一定是当前文件的第一块，因此借助RandomAccessFile这个类，我们将读取数据的指针移动到PageId的pgNo所指示的位置，在开始读取一个数据块大小的数据。



<h6>2.7 SeqScan.java</h6>
​		有了HeapFileIterator实现的基础，这个类在理解和实现上都相对容易。通过Catalog获取对应存储表的文件DbFile，再获取该DbFile的iterator()函数返回的DbFileIterator，从而达到遍历文件中所有数据块里存储的记录的目标。

![1584719311695](C:\Users\麦子\AppData\Roaming\Typora\typora-user-images\1584719311695.png)



<h3>3.遇到的问题</h3>
<h6>3.1 numPages和pagePosition之间的惨案</h6>
​		在进入ScanTest的时候，发现测试匹配元组时答案总是部分正确部分错误，通过单步调试依然没办法找出原因。因此我上网找到了别人已经写好的simpleDB的代码，开始一个个部分的校准。最关键的问题出现在pagePosition和numPages的比较中。在hasNext()这个函数中，我需要通过pagePosition的变化来获取下一个仍然存在的Page的Iterator，采用的方式是pagePosition++，同时我也要判断pagePosition的位置是否已经超过numPages的数目。在这个过程中，真正写在代码里的if()判断条件应该是pagePosition < numPages() -1，也就是说，我们需要判断的不是pagePosition是否还在numPages() 的范围里，而是当前pagePosition的下一个是否仍然为合法的数据块。

​		这个问题困扰了我两天，起因还是我没有深入地理解各个类之间的关系和每一个函数的作用。成功解决问题还是有助于我更深入的理解这次作业。



<h6>3.2 至今没有理解的Java自动将二进制数转成整数的机制</h6>
​		Java会将二进制数，即一个byte存储的数自动转成整数。这个转换机制跟网上说的方式都不吻合，因此断绝了通过这个整数进行运算从而得到记录的有效与否的念想。



<h3>4.Git Commit History</h3>
![1584719008953](C:\Users\麦子\AppData\Roaming\Typora\typora-user-images\1584719008953.png)

![1584719099537](C:\Users\麦子\AppData\Roaming\Typora\typora-user-images\1584719099537.png)

![1584719146535](C:\Users\麦子\AppData\Roaming\Typora\typora-user-images\1584719146535.png)

GitHub地址：1184649102@qq.com

GitHub用户名：VitalC-3026