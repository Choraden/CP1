package cp1.solution;

import cp1.base.ResourceId;
import cp1.base.ResourceOperation;

public class Operation {
   private ResourceId rid;
   private ResourceOperation op;

   public Operation (ResourceId rid, ResourceOperation op) {
       this.rid = rid;
       this.op = op;
   }

   public ResourceId getResourceId() {
       return rid;
   }

    public ResourceOperation getResourceOperation() {
        return op;
    }
}