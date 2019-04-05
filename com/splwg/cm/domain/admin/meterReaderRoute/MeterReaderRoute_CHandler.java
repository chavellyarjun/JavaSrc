package com.splwg.cm.domain.admin.meterReaderRoute;


import com.splwg.base.api.DataTransferObject;
import com.splwg.base.api.changehandling.AbstractChangeHandler;
import com.splwg.base.api.changehandling.ValidationRule;

/**
 *
@ChangeHandler (entityName = meterReaderRoute)
 */
public class MeterReaderRoute_CHandler extends AbstractChangeHandler<MeterReaderRoute> {
    /**
     * Cascading logic to fire before validation when an entity is being added OR changed.
     *
     * @param  businessEntity 
     * @param oldDTO the old version of the DataTransferObject, or null if action is add
     */
    public void handleAddOrChange(MeterReaderRoute businessEntity,
            DataTransferObject<MeterReaderRoute> oldDTO) {
    	super.handleAddOrChange(businessEntity, oldDTO);
    }

    /**
     * Return an array of ValidationRules that will be enforced by the
     * framework.  Building the array of rules requires application programming.
     * The invocation of this method should only be performed by the framework
     * and is performed at an unspecified time.
     *
     * @return  an array of  ValidationRules
     */

    public ValidationRule[] getValidationRules() {
        return new ValidationRule[] { };
    }

    /**
     * Allow for defaulting or other changes to be made to a DataTransferObject before
     * it is used to add a new BusinessEntity
     *
     * @param newDTO the value that may be modified by the implementation of this method.
     */
    public void prepareToAdd(DataTransferObject newDTO) {
       super.prepareToAdd(newDTO);
    }
    
    public void prepareToChange(MeterReaderRoute unchangedEntity, DataTransferObject<MeterReaderRoute> newDTO){
    	super.prepareToChange(unchangedEntity, newDTO);   	

    }
}
