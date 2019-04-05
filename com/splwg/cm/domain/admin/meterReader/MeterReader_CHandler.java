package com.splwg.cm.domain.admin.meterReader;

import java.util.Locale;

import com.splwg.base.api.DataTransferObject;
import com.splwg.base.api.changehandling.AbstractChangeHandler;
import com.splwg.base.api.changehandling.ValidationRule;

/**
 *
@ChangeHandler (entityName = meterReader)
 */
public class MeterReader_CHandler extends AbstractChangeHandler<MeterReader> {
    /**
     * Cascading logic to fire before validation when an entity is being added OR changed.
     *
     * @param  businessEntity 
     * @param oldDTO the old version of the DataTransferObject, or null if action is add
     */
    public void handleAddOrChange(MeterReader businessEntity,
            DataTransferObject<MeterReader> oldDTO) {
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
        MeterReader_DTO dto = (MeterReader_DTO) newDTO;
        //Populate Reader Name upper column
        String readerName = dto.getReaderName();
        if(notBlank(readerName)){        	
        	dto.setReaderNameInUpperCase(readerName.toUpperCase(new Locale("TUR")));
        }
        
        // Populate City Upper column
        String city = dto.getCity();
        if(notBlank(city)){        	
        	dto.setCityUppercase(city.toUpperCase(new Locale("TUR")));
        }
        /* // Encrypt password
        String passwordText = dto.getReaderPassword();
        if(notBlank(passwordText) && (!Cryptography.isEncrypted(passwordText))){
        	String encryptedPwd = Cryptography.encryptAndWrap(passwordText);
        	dto.setReaderPassword(encryptedPwd);
        }*/
    }
    
    public void prepareToChange(MeterReader unchangedEntity, DataTransferObject<MeterReader> newDTO){
    	super.prepareToChange(unchangedEntity, newDTO);
        // Encrypt password
    	MeterReader_DTO dto = (MeterReader_DTO) newDTO;
    	/* String passwordText = dto.getReaderPassword();
        if(notBlank(passwordText) && (!Cryptography.isEncrypted(passwordText))){
        	String encryptedPwd = Cryptography.encryptAndWrap(passwordText);
        	dto.setReaderPassword(encryptedPwd);
        }
    	 */        
    	if(!unchangedEntity.getReaderName().equals(dto.getReaderName())){
        	dto.setReaderNameInUpperCase(dto.getReaderName().toUpperCase(new Locale("TUR")));
        }
        if(!unchangedEntity.getCity().equals(dto.getCity())){
        	dto.setCityUppercase(dto.getCity().toUpperCase(new Locale("TUR")));
        }

    }
}
