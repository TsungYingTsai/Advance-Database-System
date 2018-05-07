
package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.Database.Transaction;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

public class SortOperator {
  private Transaction transaction;
  private String tableName;
  private Comparator<Record> comparator;
  private Schema operatorSchema;
  private int numBuffers;

  public SortOperator(Transaction transaction, String tableName, Comparator<Record> comparator) throws DatabaseException, QueryPlanException {
    this.transaction = transaction;
    this.tableName = tableName;
    this.comparator = comparator;
    this.operatorSchema = this.computeSchema();
    this.numBuffers = this.transaction.getNumMemoryPages();
  }

  public Schema computeSchema() throws QueryPlanException {
    try {
      return this.transaction.getFullyQualifiedSchema(this.tableName);
    } catch (DatabaseException var2) {
      throw new QueryPlanException(var2);
    }
  }

  public class Run {
    private String tempTableName;

    public Run() throws DatabaseException {
      this.tempTableName = SortOperator.this.transaction.createTempTable(SortOperator.this.operatorSchema);
    }

    public void addRecord(List<DataBox> values) throws DatabaseException {
      SortOperator.this.transaction.addRecord(this.tempTableName, values);
    }

    public void addRecords(List<Record> records) throws DatabaseException {
      Iterator<Record> var2 = records.iterator();

      while(var2.hasNext()) {
        Record r = var2.next();
        this.addRecord(r.getValues());
      }

    }

    public Iterator<Record> iterator() throws DatabaseException {
      return SortOperator.this.transaction.getRecordIterator(this.tempTableName);
    }

    public String tableName() {
      return this.tempTableName;
    }
  }

  public Run sortRun(Run run) throws DatabaseException {
    List<Record> list = new ArrayList();
    Iterator<Record> iter = run.iterator();

    while(iter.hasNext()) {
      list.add(iter.next());
    }

    Collections.sort(list, this.comparator);
    Run newRun = this.createRun();
    newRun.addRecords(list);
    return newRun;
  }

  public Run mergeSortedRuns(List<Run> runs) throws DatabaseException {
    Run newRun = this.createRun();
    PriorityQueue<Pair<Record, Integer>> Queue = new PriorityQueue(new RecordPairComparator());
    List<Iterator<Record>> saveIter = new ArrayList();
    int Value = 0;
    Iterator<Run> runIter = runs.iterator();

    while(runIter.hasNext()) {
      Run eachRun = runIter.next();
      Iterator<Record> ite = eachRun.iterator();
      if (ite.hasNext()) {
        saveIter.add(ite);
        Queue.add(new Pair(ite.next(), Value));
        Value += 1;
      }
    }

    for(int Qsize = Queue.size(); Qsize > 0; Qsize = Queue.size()) {
      Pair<Record, Integer> addPair = Queue.poll();
      newRun.addRecord(addPair.getFirst().getValues());
      boolean checkIte = saveIter.get((addPair.getSecond()).intValue()).hasNext();
      if (checkIte) {
        Queue.add(new Pair(saveIter.get((addPair.getSecond()).intValue()).next(), addPair.getSecond()));
      }
    }

    return newRun;
  }

  public List<Run> mergePass(List<Run> runs) throws DatabaseException {
    List<Run> RunList = new ArrayList();

    for(int i = 0; i < runs.size(); i += this.numBuffers - 1) {
      List<Run> subList = runs.subList(i, i + (this.numBuffers - 1) <= runs.size() ? i + (this.numBuffers - 1) : runs.size());
      RunList.add(this.mergeSortedRuns(subList));
    }

    return RunList;
  }

  public String sort() throws DatabaseException {
    Iterator<Page> pageIterator = this.transaction.getPageIterator(this.tableName);
    pageIterator.next();
    List<Run> sortRuns = new ArrayList();

    while(pageIterator.hasNext()) {
      Iterator<Record> blockIterator = this.transaction.getBlockIterator(this.tableName, pageIterator, this.numBuffers - 1);
      ArrayList records = new ArrayList();

      while(blockIterator.hasNext()) {
        records.add(blockIterator.next());
      }

      Run tempRun = this.createRun();
      tempRun.addRecords(records);
      Run addedRun = this.sortRun(tempRun);
      sortRuns.add(addedRun);
    }

    while(sortRuns.size() > 1) {
      sortRuns = this.mergePass(sortRuns);
    }

    return sortRuns.get((sortRuns).size() - 1).tableName();
  }

  public Run createRun() throws DatabaseException {
    return new Run();
  }

  private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
    private RecordPairComparator() {
    }

    public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
      return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());
    }
  }

}
