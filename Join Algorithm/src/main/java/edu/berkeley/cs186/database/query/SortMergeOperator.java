
package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.Database.Transaction;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.query.JoinOperator.JoinIterator;
import edu.berkeley.cs186.database.query.JoinOperator.JoinType;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.RecordIterator;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class SortMergeOperator extends JoinOperator {
  public SortMergeOperator(QueryOperator leftSource, QueryOperator rightSource, String leftColumnName, String rightColumnName, Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new SortMergeOperator.SortMergeIterator();
  }

  private class SortMergeIterator extends JoinIterator {
    private String leftTableName = this.getLeftTableName();
    private String rightTableName = this.getRightTableName();
    private RecordIterator leftIterator;
    private RecordIterator rightIterator;
    private Record leftRecord;
    private Record nextRecord;
    private Record rightRecord;
    private SortMergeOperator.SortMergeIterator.LR_RecordComparator LRCompare = new SortMergeOperator.SortMergeIterator.LR_RecordComparator();
    private boolean marked;
    private HashMap<Record, Integer> tempSave = new HashMap();

    public SortMergeIterator() throws QueryPlanException, DatabaseException {
      super();
      SortOperator leftUnSortTable = new SortOperator(SortMergeOperator.this.getTransaction(), this.leftTableName, new SortMergeOperator.SortMergeIterator.LeftRecordComparator());
      SortOperator rightUnSortTable = new SortOperator(SortMergeOperator.this.getTransaction(), this.rightTableName, new SortMergeOperator.SortMergeIterator.RightRecordComparator());
      String leftSortName = leftUnSortTable.sort();
      String rightSortName = rightUnSortTable.sort();
      this.leftIterator = SortMergeOperator.this.getRecordIterator(leftSortName);
      this.rightIterator = SortMergeOperator.this.getRecordIterator(rightSortName);
      this.leftRecord = this.leftIterator.hasNext() ? this.leftIterator.next() : null;
      this.rightRecord = this.rightIterator.hasNext() ? this.rightIterator.next() : null;
      if (this.rightRecord != null) {
        this.rightIterator.mark();
        this.marked = false;
        this.fetchNextRecord();
      }
    }

    private void resetRightRecord() {
      this.rightIterator.reset();

      assert this.rightIterator.hasNext();

      this.rightRecord = this.rightIterator.next();
      this.rightIterator.mark();
    }

    private void nextLeftRecord() throws DatabaseException {
      this.leftRecord = this.leftIterator.hasNext() ? this.leftIterator.next() : null;
    }

    private void nextRightRecord() throws DatabaseException {
      this.rightRecord = this.rightIterator.hasNext() ? this.rightIterator.next() : null;
    }

    private void fetchNextRecord() throws DatabaseException {
      if (this.leftRecord == null) {
        throw new DatabaseException("No new record to fetch");
      } else {
        this.nextRecord = null;

        do {
          while(this.leftRecord != null && this.LRCompare.compare(this.leftRecord, this.rightRecord) < 0) {
            this.nextLeftRecord();
          }

          if (this.leftRecord == null) {
            break;
          }

          while(this.rightRecord != null && this.LRCompare.compare(this.leftRecord, this.rightRecord) > 0) {
            this.nextRightRecord();
          }

          if (this.rightRecord == null) {
            break;
          }

          assert this.leftRecord.equals(this.rightRecord);

          if (!this.marked) {
            this.rightIterator.mark();
            this.marked = true;
          }

          while(this.leftRecord != null && this.LRCompare.compare(this.leftRecord, this.rightRecord) == 0) {
            while(this.LRCompare.compare(this.leftRecord, this.rightRecord) == 0) {
              List<DataBox> leftValues = new ArrayList(this.leftRecord.getValues());
              List<DataBox> rightValues = new ArrayList(this.rightRecord.getValues());
              leftValues.addAll(rightValues);
              this.nextRecord = new Record(leftValues);
              if (this.tempSave.isEmpty()) {
                this.tempSave.put(this.nextRecord, Integer.valueOf(1));
              } else {
                this.tempSave.put(this.nextRecord, (tempSave.get(this.nextRecord)).intValue() + 1);
              }

              assert this.tempSave.size() == 1;

              this.nextRightRecord();
              if (this.rightRecord == null) {
                break;
              }
            }

            this.resetRightRecord();
            this.nextLeftRecord();
            if (this.leftRecord == null) {
              break;
            }
          }
        } while(!this.hasNext());

      }
    }

    public boolean hasNext() {
      return !this.tempSave.isEmpty();
    }

    public Record next() {
      if (!this.hasNext()) {
        throw new NoSuchElementException();
      } else {
        Record name = this.nextRecord;
        if (tempSave.get(name).intValue() > 1) {
          this.tempSave.put(name, tempSave.get(name).intValue() - 1);
          return name;
        } else {
          this.marked = false;
          this.tempSave.clear();

          try {
            this.fetchNextRecord();
          } catch (DatabaseException var3) {
            ;
          }

          return name;
        }
      }
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }

    private class LR_RecordComparator implements Comparator<Record> {
      private LR_RecordComparator() {
      }

      public int compare(Record o1, Record o2) {
        return (o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex())).compareTo(o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
      }
    }

    private class RightRecordComparator implements Comparator<Record> {
      private RightRecordComparator() {
      }

      public int compare(Record o1, Record o2) {
        return (o1.getValues().get(SortMergeOperator.this.getRightColumnIndex())).compareTo(o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
      }
    }

    private class LeftRecordComparator implements Comparator<Record> {
      private LeftRecordComparator() {
      }

      public int compare(Record o1, Record o2) {
        return (o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex())).compareTo(o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
      }
    }
  }
}
