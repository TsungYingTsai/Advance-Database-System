
package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.Database.Transaction;
import edu.berkeley.cs186.database.common.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.query.JoinOperator.JoinIterator;
import edu.berkeley.cs186.database.query.JoinOperator.JoinType;
import edu.berkeley.cs186.database.table.Record;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class BNLJOperator extends JoinOperator {
  protected int numBuffers;

  public BNLJOperator(QueryOperator leftSource, QueryOperator rightSource, String leftColumnName, String rightColumnName, Transaction transaction) throws QueryPlanException, DatabaseException {
    super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.BNLJ);
    this.numBuffers = transaction.getNumMemoryPages();
  }

  public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
    return new BNLJOperator.BNLJIterator();
  }

  private class BNLJIterator extends JoinIterator {
    private BacktrackingIterator<Page> leftBlockIterator = null;
    private BacktrackingIterator<Page> rightPageIterator = null;
    private BacktrackingIterator<Record> leftRecordIterator = null;
    private BacktrackingIterator<Record> rightRecordIterator = null;
    private Record leftRecord = null;
    private Record rightRecord = null;
    private Record nextRecord = null;

    public BNLJIterator() throws QueryPlanException, DatabaseException {
      super();
      this.leftBlockIterator = BNLJOperator.this.getPageIterator(this.getLeftTableName());
      this.rightPageIterator = BNLJOperator.this.getPageIterator(this.getRightTableName());
      this.leftBlockIterator.next();
      this.rightPageIterator.next();
      this.nextRecord = null;
      this.leftRecordIterator = this.leftBlockIterator.hasNext() ? BNLJOperator.this.getBlockIterator(this.getLeftTableName(), this.leftBlockIterator, BNLJOperator.this.numBuffers - 1) : null;
      this.rightRecordIterator = this.rightPageIterator.hasNext() ? BNLJOperator.this.getBlockIterator(this.getRightTableName(), this.rightPageIterator, 1) : null;
      this.leftRecord = this.leftRecordIterator != null && this.leftRecordIterator.hasNext() ? (Record)this.leftRecordIterator.next() : null;
      this.rightRecord = this.rightRecordIterator != null && this.rightRecordIterator.hasNext() ? (Record)this.rightRecordIterator.next() : null;
      if (this.rightRecord != null && this.rightPageIterator != null && this.leftRecord != null) {
        this.rightRecordIterator.mark();
        this.rightPageIterator.mark();
        this.leftRecordIterator.mark();

        try {
          this.fetchNextRecord();
        } catch (DatabaseException var3) {
          this.nextRecord = null;
        }

      }
    }

    public boolean hasNext() {
      return this.nextRecord != null;
    }

    public Record next() {
      if (!this.hasNext()) {
        throw new NoSuchElementException();
      } else {
        Record nextRecord = this.nextRecord;

        try {
          this.fetchNextRecord();
        } catch (DatabaseException var3) {
          this.nextRecord = null;
        }

        return nextRecord;
      }
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }

    private void fetchNextRecord() throws DatabaseException {
      if (this.leftRecord == null) {
        throw new DatabaseException("No new record to fetch");
      } else {
        this.nextRecord = null;

        do {
          if (this.rightRecord != null) {
            DataBox leftJoinValue = this.leftRecord.getValues().get(BNLJOperator.this.getLeftColumnIndex());
            DataBox rightJoinValue = this.rightRecord.getValues().get(BNLJOperator.this.getRightColumnIndex());
            if (leftJoinValue.equals(rightJoinValue)) {
              List<DataBox> leftValues = new ArrayList(this.leftRecord.getValues());
              List<DataBox> rightValues = new ArrayList(this.rightRecord.getValues());
              leftValues.addAll(rightValues);
              this.nextRecord = new Record(leftValues);
            }

            this.rightRecord = this.rightRecordIterator.hasNext() ? (Record)this.rightRecordIterator.next() : null;
          } else if (this.Isnextleftrecord()) {
            this.nextLeftRecord();
            this.resetRightRecord();
          } else if (this.IsnextRightPage()) {
            this.leftRecordReset();
            this.nextRightPage();
          } else {
            if (!this.IsnextLeftBlock()) {
              break;
            }

            this.nextLeftBlock();
            this.resetRightPage();
          }
        } while(!this.hasNext());

      }
    }

    private void resetRightPage() throws DatabaseException {
      this.rightPageIterator.reset();
      this.rightRecordIterator = BNLJOperator.this.getBlockIterator(this.getRightTableName(), this.rightPageIterator, 1);
      this.rightRecord = this.rightRecordIterator.next();
      this.rightRecordIterator.mark();
    }

    private void nextRightPage() throws DatabaseException {
      this.rightRecordIterator = BNLJOperator.this.getBlockIterator(this.getRightTableName(), this.rightPageIterator, 1);
      this.rightRecord = this.rightRecordIterator.next();
      this.rightRecordIterator.mark();
    }

    private void leftRecordReset() {
      this.leftRecordIterator.reset();

      assert this.leftRecordIterator.hasNext();

      this.leftRecord = this.leftRecordIterator.next();
      this.leftRecordIterator.mark();
    }

    private void resetRightRecord() {
      this.rightRecordIterator.reset();

      assert this.rightRecordIterator.hasNext();

      this.rightRecord = this.rightRecordIterator.next();
      this.rightRecordIterator.mark();
    }

    private void nextLeftRecord() throws DatabaseException {
      this.leftRecord = this.leftRecordIterator.next();
    }

    private void nextLeftBlock() throws DatabaseException {
      if (!this.leftBlockIterator.hasNext()) {
        throw new DatabaseException("All Done!");
      } else {
        this.leftRecordIterator = BNLJOperator.this.getBlockIterator(this.getLeftTableName(), this.leftBlockIterator, BNLJOperator.this.numBuffers - 1);
        this.leftRecord = this.leftRecordIterator.next();
        this.leftRecordIterator.mark();
      }
    }

    private boolean Isnextleftrecord() throws DatabaseException {
      return this.leftRecordIterator.hasNext();
    }

    private boolean IsnextRightPage() throws DatabaseException {
      return this.rightPageIterator.hasNext();
    }

    private boolean IsnextLeftBlock() throws DatabaseException {
      return this.leftBlockIterator.hasNext();
    }
  }
}
