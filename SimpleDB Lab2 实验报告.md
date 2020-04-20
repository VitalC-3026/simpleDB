<h1>SimpleDB Lab2 实验报告</h1>

<p align="right">1811499 麦隽韵</p>

<h3>1. 整体框架</h3>

​        本次实验需要完成Filter、Join、Aggregator、StringAggregator、IntegerAggregator、HeapFile和HeapPage中的insert()、delete()、Insert、Delete、Bufferpool中除了与事务处理相关的其他函数。本次实验最重要的是实现数据库管理系统中DML语言的功能，使得使用者可以通过SQL语句对数据进行操作。

​        其中，Filter是过滤，通过Predicate.Op定义的遴选办法，将不匹配Predicate的元组去除，返回需要的所有元组；Join是笛卡尔积，通过JoinPredicate设置进行笛卡尔积的条件，通过Join这个操作符返回完成笛卡尔积的元组。 Aggregator是聚合操作符，通过调用具体不同类型聚合函数接口，笼统地返回聚合结果。而StringAggregator和IntegerAggregator则是根据不同类型进行具体的聚合操作处理。Insert和Delete类是插入和删除元组的操作符，会返回进行操作后的结果反馈，而具体进行对物理层数据进行增加和删除操作的则是通过BufferPool的插入和删除函数进行具体执行。而BufferPool中的插入和删除函数则是通过tableId获取到相应的文件，在具体文件中获取具体处理的页完成增加和删除操作，然后将写入脏数据的脏页存储到BufferPool中，并根据一定条件一次性再将脏页写回到文件里。同时考虑到BufferPool的容量有限，还需要设计Eviction()根据情况抛出多余的页。

<h3>2. 设计思路</h3>

<h6>2.1 Predicate.java & Filter.java</h6>

​        在构造Predicate类的实例时，我们需要传入操作的类型Op，一个字段Field，以及标识字段所在位置的location。一开始我对三个传入的参数感到非常的困惑。而在该类的filter函数里就可以发现，通过传入的Tuple在指定的location位置上获取到该元组的字段，然后依据当前Predicate类规定的操作类型Op，和Predicate指定的字段进行比较，如果满足条件则要这个传入的Tuple，否则就把它过滤掉。

​		而Filter类就是一个使用Predicate实例进行筛选元组，并返回符合条件元组的一个操作符。它的本质和lab1实现的SeqScan一样，需要遍历OpIterator中存放的所有元组，只不过Filter会依据Predicate中Op的判断条件进行元组的筛选。

​		在实现Predicate类值得注意的小细节是，filter函数中必须调用的是传入Tuple的比较函数，这与测试数据的设置有关。

![1586419186294](C:\Users\麦子\AppData\Roaming\Typora\typora-user-images\1586419186294.png)



<h6>2.2 JoinPredicate.java & Join.java</h6>

​		JoinPredicate的实现跟Predicate非常类似，但这个类中的filter函数目的是为了比较任意两个Tuple指定的Field是否满足Op的条件，从而决定是否可以进行笛卡尔积，而不是和默认的Field进行比较以筛选出匹配的元组。

​		在Join类中构造时传入了两个OpIterator，相当于传入了两张表，这两张表的任意两行记录都需要进行合并。以下图为例，R表有四行数据，S表有四行数据，现在JoinPredicate构造时传入的参数field1=2，即指向R关系的第二个字段b，field2=1，即指向S关系的第一个字段b，Op为EQUALS。而Join构造时传入的是上述的JoinPredicate以及存有R表四个元组的OpIterator child1，存有S表四个元组的OpIterator child2。再没有进行任何优化的情况下做笛卡尔积，实际上就是两重循环。如果满足条件，即JoinPredicate的filter函数返回值为真，那么就将这两个元组进行合并，得到一个新元组。如果child2遍历完一次后，要进行rewind后再跟下一个child1的元组进行笛卡尔积并检验是否符合条件。

![1586419831206](C:\Users\麦子\AppData\Roaming\Typora\typora-user-images\1586419831206.png)

​		但在写Join类中的fetchNext()函数时，希望的是R.1与S.2匹配并返回结果后，下一次就不再返回R.1与S.2，而是从R.1与S.3开始进行继续遍历。但显然测试并不是这样设置的。当actual Tuple成功匹配expected Tuple时，Join操作符会重新打开，这时候原先存储的结果都会清空，从头逐一做笛卡尔积再和expected Tuple进行对比。尽管这样会导致复杂度变高，但这是保证数据安全性的做法，这样我在任何时候进行rewind都保证结果的一致性。

​		所以我的fetchNext设计的很简单，将每一次变化的tuple都保留再Join类中，如果不像测试中每成功匹配一次就将Join进行rewind，那么它就可以遍历完R表和S表4×4=16种结果，并将符合条件的Tuple筛选出来。

​		假设遍历R表为外循环，遍历S表为内循环，tuple1记录R表的元组，tuple2记录S表的元组。循环开始，第一个符合条件的是R.1和S.2，此时tuple2记录的是S.2。当fetchNext()再次被调用的时候，tuple1和tuple2是不会因为这个操作符被关闭再打开而失去数据的，因此依然会进入第一层循环，但进入第一重循环时满足了child1.hasNext()和tuple2 != null，这时候我们需要通过tuple2是否为null来判断对于R.1，是否还有没有与R.1进行笛卡尔积的S表中的元组。因此在进入第二重循环有一个if判断，以决定tuple1是继续为R.1还是要变到R.2。

​		若循环进行到R.4和S.1，发现是一个满足条件的合并，那就会返回新值，但此时child1已经没有下一个元素了，可是R和S其实还没有做完笛卡尔积，所以第一重循环的tuple2 != null这时候就会起作用，使之可以进入循环继续遍历R.4和S.2、S.3、S.4。

```Java
protected Tuple fetchNext() throws TransactionAbortedException, DbException, NoSuchFieldException, IOException {
        // some code goes here
    while (child1.hasNext() || tuple2 != null) {
        if(tuple2 == null) {
            tuple1 = child1.next();
        }
        while (child2.hasNext()) {
            tuple2 = child2.next();
            if (p.filter(tuple1, tuple2)) {
                Tuple newTuple = new Tuple(TupleDesc.merge(tuple1.getTupleDesc(), tuple2.getTupleDesc()));
                // OpIterator is iterator of tuples with same TupleDesc? Yes
                // Tuple tuple = new Tuple(getTupleDesc());
                int count = 0;
                for (int i = 0; i < tuple1.getTupleDesc().numFields(); i++) {
                    newTuple.setField(count++, tuple1.getField(i));
                }
                for (int i = 0; i < tuple2.getTupleDesc().numFields(); i++) {
                    newTuple.setField(count++, tuple2.getField(i));
                }
                return newTuple;
            }
        }
        child2.rewind();
        tuple2 = null;
    }
    return null;
}
```



<h6>2.3 IntegerAggregator & StringAggregator</h6>

​		这两个类实现的是聚集操作，针对整数类型和字符串类型进行相应的聚集。两个类中最重要的是mergeTupleIntoGroup的设计。因为StringAggregator主要是根据Group By的字段进行字符串计数，和IntegerAggregator的功能有重叠，因此只根据IntegerAggregator为例进行设计思路阐述。

​		首先IntegerAggregator构造传参时传入了需要Group By的字段位置，字段类型，需要Aggregate的字段位置以及需要Aggregate的类型Aggregator.Op。如果Aggregate的类型不需要进行分组的话，那就只需要将传入的元组所属模式与之前已经处理过的元组所属模式进行对比，一致的话根据Aggregator.Op进行处理。如果需要进行分组的话，我设计了一个HashMap，键存放Group By的对象，值存放的是根据Aggregator.Op进行处理后的结果值。如果聚集类型是平均值，则需要将此次调用mergeTupleIntoGroup之前传入的所有Tuple值保留下来，就行平均值的计算。但也有一个更节省空间的办法就是在聚集类型是平均值的时候也把元组总数记录下来，然后当新的Tuple值传入的时候，平均值的计算就为(avg × count + newTupleValue) / (count + 1)。我在设计的时候写了一个aggregateManipulation函数专门处理不同类型的聚集。不将这个处理直接放在mergeTupleIntoGroup上是可以将来新增聚集类型时减少代码的耦合修改。

​		这里有两个小细节处理。一个是由于不清楚Group By到底是什么类型，因此在HashMap的泛型对键设置为Object。根据测试的样例来看，这个键只有Integer和String两种类型，因此在mergeTupleIntoGroup函数中进行了判断和强制类型转换，因为Field的接口是没有getValue这个函数的，因此无法将Group By对象的具体指存入HashMap种作为键。具体的操作是如下这两段代码。

```java
if (gbFieldType.equals(Type.STRING_TYPE)){
    gFieldValue = ((StringField) tup.getField(gbField)).getValue();
}
if (gbFieldType.equals(Type.INT_TYPE)){
    gFieldValue = ((IntField) tup.getField(gbField)).getValue();
}
```

​		第二个小细节是在返回结果的时候，在需要Group By的前提下，我只需要一个有两个字段的元组，分别为Group By的字段和Aggregate结果的字段。因此我需要在mergeTupleIntoGroup函数中进行新元组的创建。尽管传入的元组的模式说明和返回的结果的模式说明是一致的，但我必须要考虑到将来如果传进来的元组有多于两个字段的数据，我不能贸然的将传进来的元组的模式修改成只有两个字段，为我所用作为返回结果，因为这会破坏元组结构。新创建结果元组的代码如下，主要是通过创建newTupleDesc和tuple.setField(1, new IntField(newInteger));来实现。

```java
Type[] types = new Type[2];
String[] names = new String[2];
names[0] = gbFieldType.name();
types[0] = gbFieldType;
names[1] = aFieldType.name();
types[1] = aFieldType;
TupleDesc newTupleDesc = new TupleDesc(types, names);
……
int newInteger = aggregateManipulation(what, tup);
int oldInteger = aggregateIntegers.get(gFieldValue);
aggregateIntegers.replace(gFieldValue, oldInteger, newInteger);
if (newInteger != oldInteger) {
tuple.setField(1, new IntField(newInteger));
aggregateTuples.put(gFieldValue, tuple);
```

​		具体的mergeTupleIntoGroup函数代码可以参看附件的源代码。



<h6>2.4 Aggregate.java</h6>

​		Aggregate类是一个聚合的操作符，主要作用就是调用各类型的Aggregator得到聚合好的结果然后返回。重点在于fetchNext()函数的处理。

​		为了保存聚合好的元组结果，我设置了integerAggregator和stringAggregator来作为是否进行OpIterator遍历的判断标志，同时用result保存了OpIterator遍历过一次之后所有元组结果。

```java
protected Tuple fetchNext() throws TransactionAbortedException, DbException, NoSuchFieldException, IOException {
    // some code goes here
    if (integerAggregator == null && stringAggregator == null) {
        // 遍历OpIterator child的代码，即进行聚合
        while (child.hasNext()) {...}
        if(integerAggregator != null) {
            result = integerAggregator.iterator();
            result.open();
        }
        if(stringAggregator != null) {
            result = stringAggregator.iterator();
            result.open();
        }
    }
    if(result.hasNext()) {
        return result.next();
    }
    return null;
}
```

​		而针对具体的遍历需要进行聚合操作的元组，我需要对元组的类型进行区分，以判断出是进行整数类型还是字符串类型的聚合。然后再对是否Group By进行不同的处理，因此源代码中可以看到有比较多的if语句判断。分类处理好后就是调用Aggregator的mergeTupleIntoGroup函数进行处理。

```java
while (child.hasNext()) {
    outputTuple = child.next();
    if(outputTuple.getField(afield).getType().equals(Type.INT_TYPE)) {
        if(integerAggregator == null){
            Type gType;
            if(gfield == Aggregator.NO_GROUPING) {
                gType = Type.INT_TYPE;
            } else {
                gType = outputTuple.getField(gfield).getType();
            }
            integerAggregator = new IntegerAggregator(gfield, gType, afield, aop);
            integerAggregator.mergeTupleIntoGroup(outputTuple);
        } else {
            integerAggregator.mergeTupleIntoGroup(outputTuple);
        }
    }
    if(outputTuple.getField(afield).getType().equals(Type.STRING_TYPE)) {
        if(stringAggregator == null) {
            Type gType;
            if(gfield == Aggregator.NO_GROUPING) {
                gType = Type.INT_TYPE;
            } else {
                gType = outputTuple.getField(gfield).getType();
            }
            stringAggregator = new StringAggregator(gfield, gType, afield, aop);
            stringAggregator.mergeTupleIntoGroup(outputTuple);
        } else {
            stringAggregator.mergeTupleIntoGroup(outputTuple);
        }
    }
}
if(integerAggregator != null) {
    result = integerAggregator.iterator();
    result.open();
}
if(stringAggregator != null) {
    result = stringAggregator.iterator();
    result.open();
}
```



<h6>2.5 HeapPage</h6>

​		HeapPage类需要对insertTuple和deleteTuple函数进行设计，同时在增加和删除元组的同时，同步的修改该数据页中记录可用数据槽的bitmap以及标记脏页。插入元组的函数的设计如下：找到该页可用的数据槽完成插入，同时对插入的元组的标识RecordId进行修改，调用markSlotUsed()函数修改bitmap。删除元组的函数的设计如下：根据传入的元组的RecordId的TupleNumber值进行删除，并修改bitmap。

​		标记脏页的函数主要是保存是否为脏页这个值以及事务Id，但该函数并不用在insertTuple或deleteTuple函数中进行调用。

​		修改bitmap的函数markSlotUsed的设计如下：对传入的元组所在位置i进行整除8和模8的计算，得到元组所对应bitmap的位置，然后通过移位运算，将不需要修改的bit转换成十进制保存到newSlotInfo这个临时变量里，等到需要修改的元组所在位置的bit时，根据传入的mark value进行修改，再转换成十进制保存到newSlotInfo中，最后将这个十进制的newSlotInfo进行强制类型转换成byte型，存入bitmap对应的位置。源代码如下：

```java
private void markSlotUsed(int i, boolean value) {
    // some code goes here
    // not necessary for lab1
    int headerGroup = i / 8;
    int location = i % 8;
    byte slotInfo = header[headerGroup];
    int newSlotInfo = 0;
    int[] newHeader = new int[8];
    for (int j = 0 ; j < 8; j++){
        if (j == location) {
            if (value) {
                newHeader[7 - j] = 1;
            } else {
                newHeader[7 - j] = 0;
            }
            newSlotInfo +=  (int)Math.pow(2, j) * newHeader[7 - j];
            continue;
        }
        if (((slotInfo >> j) & 1) == 1){
            newHeader[7 - j] = 1;
        } else {
            newHeader[7 - j] = 0;
        }
        newSlotInfo +=  (int)Math.pow(2, j) * newHeader[7 - j];
    }
    header[headerGroup] = (byte) newSlotInfo;
}
```



<h6>2.6 HeapFile</h6>

​		HeapFile类需要修改deleteTuple、insertTuple、writePage函数。writePage函数与readPage函数设计类似，同样是使用RandomAccessFile类，通过seek函数将指针跳转到文件里应该要写入该数据页的位置，完成数据页在文件中的写入操作。

​		insertTuple函数需要从BufferPool中获取一个有空数据槽的数据页，调用数据页的insertTuple函数完成元组的插入，并调用数据页的markDirty标记这个数据页为脏页，并将其加入到函数返回的结果集。但是如果不巧，BufferPool中所有的数据页都没有位置允许插入元组了，那就需要新建一个空白的数据页进行元组的插入。这里需要留意HeapPage中有一个createEmptyPageData的函数，作为构造空白HeapPage的传参之一。而创建了新的数据页后，要先写到文件里，也就是调用writePage函数，这样这个元组才实际的被插入到磁盘里。否则这个元组是无法被找到的。接下来还需要将该新的数据页放到BufferPool里，以便允许再有新的元组插入时，有一个有空位的数据页可以被读取。如果不插入到BufferPool里，那么BufferPool里永远都是当前这个文件里写满元组的数据页。下一次再插入元组时，就会再次新建一个新的数据页，而没有充分利用好上一个新建的有较多空数据槽的数据页。这时候因为空白数据页刚刚被写入磁盘，所以暂时不需要被记录为脏页。源码见下方。

​		deleteTuple函数设计上相对比较简单，只需要在BufferPool中找到传入元组所在的数据页，调用HeapPage的deleteTuple函数，将该页设置为脏页，并返回结果即可。

```java
public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException, NoSuchFieldException {
    // some code goes here
    // not necessary for lab1
    ArrayList<Page> pages = new ArrayList<>();
    for (int i = 0; i < numPages(); i++) {
        HeapPageId heapPageId = new HeapPageId(getId(), i);
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
        if (heapPage.getNumEmptySlots() > 0) {
            heapPage.insertTuple(t);
            heapPage.markDirty(true, tid);
            pages.add(heapPage);
            break;
        }
    }
    // 创建新的blank paper的时候要先写入文件，再从buffer pool取出来，再insert tuple
    if (pages.size() == 0) {
        HeapPageId newPageId = new HeapPageId(getId(), numPages());
        HeapPage newPage = new HeapPage(newPageId, HeapPage.createEmptyPageData());
        writePage(newPage);
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, newPageId, Permissions.READ_WRITE);
        // heapPage.markDirty(true, tid); 已经写入文件了，不需要再标记
        heapPage.insertTuple(t);
        pages.add(heapPage);
    }
    return pages;
}
```



<h6>2.7 BufferPool.java</h6>

​		首先实现的是insertTuple和deleteTuple，通过传入的tableId可以在catalog中找到对应的文件，并向该文件插入或删除Tuple，将返回的更改过的Page值加入到BufferPool中，并标记该数据页为脏页。这里依然需要标记是因为在EvictionTest中文件的InsertTuple被重写了，所以在这里再次标识一下比较保险。

​		然后实现flushPage和flushAllPage函数。flushAllPage函数就是遍历整个BufferPool中所有数据块，进行flushPage操作。而flushPage函数就是从BufferPool中取出对应传参PageId的数据页，并检查其是否为脏页，如果是的话就通过Catalog获取该数据页所在的文件，将它写入文件中并修改脏页状态。

​		最后实现的是EvictPage函数。这个函数是在当BufferPool满了之后随机去除一个数据页，但值得注意的是，在去除这个数据页时，如果该数据页是脏页，那么就要及时地对物理底层的文件进行数据更新方可以移除，否则就会出现数据更新不一致的错误。而随机去除一个数据页的思路，我设计的并不复杂，就是使用Random类采用随机数去除一个数据页。源代码如下：

```java
private synchronized void evictPage() throws DbException, IOException, NoSuchFieldException {
    // some code goes here
    // not necessary for lab1
    Random r = new Random();
    int page2evict = r.nextInt(numPages);
    Iterator<PageId> it = bufferPoolEdit.keySet().iterator();
    int i = 0;
    while (it.hasNext() && i++ < page2evict - 1) {
        it.next();
    }
    PageId pageId = it.next();
    flushPage(pageId);
    bufferPoolEdit.remove(pageId);
}
```

​		但在测试EvictionTest的时候，我因为消耗了Java虚拟机的内存过大而一直无法通过。但我觉得我的剔除数据页设计并没有问题。后来我通读代码后发现，我在设计文件HeapFile的Iterator时，会将一个文件里的所有数据页的所有Tuple在打开该DbFileIterator的时候全部获取到，这实际上占用了很多不必要的内存空间。在Lab1的实验指导书中也提到过不需要一开始就将所有的元组存到这个DbFileIterator中。于是，我进行了如下修改，在tupleIterator没有下一个的时候，再根据当前所在的数据页位置去获取对应的文件里的数据页。即对HeapFileIterator implements DbFileIterator的hasNext函数进行了如下修改。最终我愉快的通过了最终测试。

```java
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
```



<h3>3. git commit history</h3>

![1586442213583](C:\Users\麦子\AppData\Roaming\Typora\typora-user-images\1586442213583.png)

![1586442247198](C:\Users\麦子\AppData\Roaming\Typora\typora-user-images\1586442247198.png)

![1586442274814](C:\Users\麦子\AppData\Roaming\Typora\typora-user-images\1586442274814.png)

![1586442306984](C:\Users\麦子\AppData\Roaming\Typora\typora-user-images\1586442306984.png)

![1586442332784](C:\Users\麦子\AppData\Roaming\Typora\typora-user-images\1586442332784.png)



clone：https://github.com/VitalC-3026/simpleDB.git