package com.splwg.cm.domain.usage.usageTransaction.data;

import java.util.List;


import com.ibm.icu.math.BigDecimal;
import com.splwg.base.api.datatypes.DateTime;
import com.splwg.base.api.datatypes.TimeInterval;
import com.splwg.base.domain.common.extendedLookupValue.ExtendedLookupValue_Id;
import com.splwg.d2.api.lookup.UsageTypeD2Lookup;

/**
 * @author Abjayon
 *
 * This java component will hold the input data for Summary Usage Periods
 *
 */

public class CmSummaryUsagePeriodsOutputData {

    private List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod> summaryUsagePeriodList = null;

    public static class SummaryUsagePeriod {
        private BigDecimal sequence = null;
        private DateTime startDateTime = null;
        private DateTime endDateTime = null;
        private DateTime standardStartDateTime = null;
        private DateTime standardEndDateTime = null;
        private UsageTypeD2Lookup usageType = null;
        private List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity> serviceQuantityList = null;
        private List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.Item> itemList = null;
        private List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity> sPServiceQuantityList = null;
        private List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.SPItem> spItemList = null;

        public static class ServiceQuantity {
            private BigDecimal sequence;
            private String uom;
            private String tou;
            private String sqi;
            private BigDecimal quantity;
            private String spId;
            private TimeInterval secondsPerInterval;
            
            private String routeId;
			private String serialNumber;
            private BigDecimal currentYearConsumption;
            private BigDecimal lastYearConsumption;
            private String meterId;
            private String meterBrand;
            private String meterType;
            private BigDecimal lastIndex;
            private BigDecimal firstIndex;
            private BigDecimal multiplier;
            private BigDecimal consumption;
            private BigDecimal transformerLoss;
            private BigDecimal dailyAverageUsage;
            private DateTime lastReadingDate;
            private DateTime firstReadingDate;
            private BigDecimal demandIndex;
            private BigDecimal demandMultiplier;
            private BigDecimal demandConsumption;
            private BigDecimal currentTransformerRatio;
            private BigDecimal voltageTransformerRatio;
			private BigDecimal indexDifference;
            private BigDecimal netGeneration;
            private BigDecimal generationDemand;
            private BigDecimal inductiveRatio;
            private BigDecimal capacitiveRatio;
           
            
            private List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Interval> intervalList = null;
            private List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight> highlightList = null;

            public static class Interval {
                private BigDecimal sequence = null;
                private DateTime dateTime = null;
                private BigDecimal quantity = null;
                private ExtendedLookupValue_Id condition = null;

                public ExtendedLookupValue_Id getCondition() {
                    return condition;
                }

                public void setCondition(ExtendedLookupValue_Id condition) {
                    this.condition = condition;
                }

                public DateTime getDateTime() {
                    return dateTime;
                }

                public void setDateTime(DateTime dateTime) {
                    this.dateTime = dateTime;
                }

                public BigDecimal getQuantity() {
                    return quantity;
                }

                public void setQuantity(BigDecimal quantity) {
                    this.quantity = quantity;
                }

                public BigDecimal getSequence() {
                    return sequence;
                }

                public void setSequence(BigDecimal sequence) {
                    this.sequence = sequence;
                }
            }

            public static class Highlight {
                private BigDecimal sequence = null;
                private DateTime highlightDateTime = null;
                private ExtendedLookupValue_Id highlightType = null;
                private ExtendedLookupValue_Id highlightCondition = null;
                private ExtendedLookupValue_Id highlightDerivedCondition = null;

                public BigDecimal getSequence() {
                    return sequence;
                }

                public void setSequence(BigDecimal sequence) {
                    this.sequence = sequence;
                }

                public DateTime getHighlightDateTime() {
                    return highlightDateTime;
                }

                public void setHighlightDateTime(DateTime highlightDateTime) {
                    this.highlightDateTime = highlightDateTime;
                }

                public ExtendedLookupValue_Id getHighlightType() {
                    return highlightType;
                }

                public void setHighlightType(ExtendedLookupValue_Id highlightType) {
                    this.highlightType = highlightType;
                }

                public ExtendedLookupValue_Id getHighlightCondition() {
                    return highlightCondition;
                }

                public void setHighlightCondition(ExtendedLookupValue_Id highlightCondition) {
                    this.highlightCondition = highlightCondition;
                }

                public ExtendedLookupValue_Id getHighlightDerivedCondition() {
                    return highlightDerivedCondition;
                }

                public void setHighlightDerivedCondition(ExtendedLookupValue_Id highlightDerivedCondition) {
                    this.highlightDerivedCondition = highlightDerivedCondition;
                }

            }

            public List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight> getHighlightList() {
                return highlightList;
            }

            public void setHighlightList(
                    List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Highlight> highlightList) {
                this.highlightList = highlightList;
            }

            public List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Interval> getIntervalList() {
                return intervalList;
            }

            public void setIntervalList(
                    List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity.Interval> intervalList) {
                this.intervalList = intervalList;
            }

            public BigDecimal getQuantity() {
                return quantity;
            }

            public void setQuantity(BigDecimal quantity) {
                this.quantity = quantity;
            }

            public TimeInterval getSecondsPerInterval() {
                return secondsPerInterval;
            }

            public void setSecondsPerInterval(TimeInterval secondsPerInterval) {
                this.secondsPerInterval = secondsPerInterval;
            }

            public BigDecimal getSequence() {
                return sequence;
            }

            public void setSequence(BigDecimal sequence) {
                this.sequence = sequence;
            }

            public String getSpId() {
                return spId;
            }

            public void setSpId(String spId) {
                this.spId = spId;
            }

            public String getSqi() {
                return sqi;
            }

            public void setSqi(String sqi) {
                this.sqi = sqi;
            }

            public String getTou() {
                return tou;
            }

            public void setTou(String tou) {
                this.tou = tou;
            }

            public String getUom() {
                return uom;
            }

            public void setUom(String uom) {
                this.uom = uom;
            }
            
            public String getRouteId() {
				return routeId;
			}

			public void setRouteId(String routeId) {
				this.routeId = routeId;
			}

			public String getSerialNumber() {
				return serialNumber;
			}

			public void setSerialNumber(String serialNumber) {
				this.serialNumber = serialNumber;
			}

			public BigDecimal getCurrentYearConsumption() {
				return currentYearConsumption;
			}

			public void setCurrentYearConsumption(BigDecimal currentYearConsumption) {
				this.currentYearConsumption = currentYearConsumption;
			}

			public BigDecimal getLastYearConsumption() {
				return lastYearConsumption;
			}

			public void setLastYearConsumption(BigDecimal lastYearConsumption) {
				this.lastYearConsumption = lastYearConsumption;
			}

			public String getMeterId() {
				return meterId;
			}

			public void setMeterId(String meterId) {
				this.meterId = meterId;
			}

			public String getMeterBrand() {
				return meterBrand;
			}

			public void setMeterBrand(String meterBrand) {
				this.meterBrand = meterBrand;
			}

			public String getMeterType() {
				return meterType;
			}

			public void setMeterType(String meterType) {
				this.meterType = meterType;
			}

			public BigDecimal getLastIndex() {
				return lastIndex;
			}

			public void setLastIndex(BigDecimal lastIndex) {
				this.lastIndex = lastIndex;
			}

			public BigDecimal getFirstIndex() {
				return firstIndex;
			}

			public void setFirstIndex(BigDecimal firstIndex) {
				this.firstIndex = firstIndex;
			}

			public BigDecimal getMultiplier() {
				return multiplier;
			}

			public void setMultiplier(BigDecimal multiplier) {
				this.multiplier = multiplier;
			}

			public BigDecimal getConsumption() {
				return consumption;
			}

			public void setConsumption(BigDecimal consumption) {
				this.consumption = consumption;
			}

			public BigDecimal getTransformerLoss() {
				return transformerLoss;
			}

			public void setTransformerLoss(BigDecimal transformerLoss) {
				this.transformerLoss = transformerLoss;
			}

			public BigDecimal getDailyAverageUsage() {
				return dailyAverageUsage;
			}

			public void setDailyAverageUsage(BigDecimal dailyAverageUsage) {
				this.dailyAverageUsage = dailyAverageUsage;
			}

			public DateTime getLastReadingDate() {
				return lastReadingDate;
			}

			public void setLastReadingDate(DateTime lastReadingDate) {
				this.lastReadingDate = lastReadingDate;
			}

			public DateTime getFirstReadingDate() {
				return firstReadingDate;
			}

			public void setFirstReadingDate(DateTime firstReadingDate) {
				this.firstReadingDate = firstReadingDate;
			}

			public BigDecimal getDemandIndex() {
				return demandIndex;
			}

			public void setDemandIndex(BigDecimal demandIndex) {
				this.demandIndex = demandIndex;
			}

			public BigDecimal getDemandMultiplier() {
				return demandMultiplier;
			}

			public void setDemandMultiplier(BigDecimal demandMultiplier) {
				this.demandMultiplier = demandMultiplier;
			}

			public BigDecimal getDemandConsumption() {
				return demandConsumption;
			}

			public void setDemandConsumption(BigDecimal demandConsumption) {
				this.demandConsumption = demandConsumption;
			}

			public BigDecimal getCurrentTransformerRatio() {
				return currentTransformerRatio;
			}

			public void setCurrentTransformerRatio(BigDecimal currentTransformerRatio) {
				this.currentTransformerRatio = currentTransformerRatio;
			}

			public BigDecimal getVoltageTransformerRatio() {
				return voltageTransformerRatio;
			}

			public void setVoltageTransformerRatio(BigDecimal voltageTransformerRatio) {
				this.voltageTransformerRatio = voltageTransformerRatio;
			}

			public BigDecimal getIndexDifference() {
				return indexDifference;
			}

			public void setIndexDifference(BigDecimal indexDifference) {
				this.indexDifference = indexDifference;
			}

			public BigDecimal getNetGeneration() {
				return netGeneration;
			}

			public void setNetGeneration(BigDecimal netGeneration) {
				this.netGeneration = netGeneration;
			}

			public BigDecimal getGenerationDemand() {
				return generationDemand;
			}

			public void setGenerationDemand(BigDecimal generationDemand) {
				this.generationDemand = generationDemand;
			}

			public BigDecimal getInductiveRatio() {
				return inductiveRatio;
			}

			public void setInductiveRatio(BigDecimal inductiveRatio) {
				this.inductiveRatio = inductiveRatio;
			}

			public BigDecimal getCapacitiveRatio() {
				return capacitiveRatio;
			}

			public void setCapacitiveRatio(BigDecimal capacitiveRatio) {
				this.capacitiveRatio = capacitiveRatio;
			}
        }

        public static class Item {
            private BigDecimal itemSequence;
            private String itemType;
            private BigDecimal itemCount;
            private DateTime startDateTime;
            private DateTime endDateTime;
            private BigDecimal dailyServiceQuantity;
            private String uom;

            public DateTime getEndDateTime() {
                return endDateTime;
            }

            public void setEndDateTime(DateTime endDateTime) {
                this.endDateTime = endDateTime;
            }

            public BigDecimal getItemCount() {
                return itemCount;
            }

            public void setItemCount(BigDecimal itemCount) {
                this.itemCount = itemCount;
            }

            public BigDecimal getItemSequence() {
                return itemSequence;
            }

            public void setItemSequence(BigDecimal itemSequence) {
                this.itemSequence = itemSequence;
            }

            public String getItemType() {
                return itemType;
            }

            public void setItemType(String itemType) {
                this.itemType = itemType;
            }

            public DateTime getStartDateTime() {
                return startDateTime;
            }

            public void setStartDateTime(DateTime startDateTime) {
                this.startDateTime = startDateTime;
            }

            public String getUom() {
                return uom;
            }

            public void setUom(String uom) {
                this.uom = uom;
            }

            public BigDecimal getDailyServiceQuantity() {
                return dailyServiceQuantity;
            }

            public void setDailyServiceQuantity(BigDecimal dailyServiceQuantity) {
                this.dailyServiceQuantity = dailyServiceQuantity;
            }

        }

        public static class SPItem {
            private BigDecimal itemSequence;
            private String itemType;
            private BigDecimal itemCount;
            private DateTime startDateTime;
            private DateTime endDateTime;
            private BigDecimal quantity;
            private String uom;
            private String spId;

            public DateTime getEndDateTime() {
                return endDateTime;
            }

            public void setEndDateTime(DateTime endDateTime) {
                this.endDateTime = endDateTime;
            }

            public BigDecimal getItemCount() {
                return itemCount;
            }

            public void setItemCount(BigDecimal itemCount) {
                this.itemCount = itemCount;
            }

            public BigDecimal getItemSequence() {
                return itemSequence;
            }

            public void setItemSequence(BigDecimal itemSequence) {
                this.itemSequence = itemSequence;
            }

            public String getItemType() {
                return itemType;
            }

            public void setItemType(String itemType) {
                this.itemType = itemType;
            }

            public BigDecimal getQuantity() {
                return quantity;
            }

            public void setQuantity(BigDecimal quantity) {
                this.quantity = quantity;
            }

            public String getSpId() {
                return spId;
            }

            public void setSpId(String spId) {
                this.spId = spId;
            }

            public DateTime getStartDateTime() {
                return startDateTime;
            }

            public void setStartDateTime(DateTime startDateTime) {
                this.startDateTime = startDateTime;
            }

            public String getUom() {
                return uom;
            }

            public void setUom(String uom) {
                this.uom = uom;
            }

        }

        public DateTime getEndDateTime() {
            return endDateTime;
        }

        public void setEndDateTime(DateTime endDateTime) {
            this.endDateTime = endDateTime;
        }

        public BigDecimal getSequence() {
            return sequence;
        }

        public void setSequence(BigDecimal sequence) {
            this.sequence = sequence;
        }

        public DateTime getStandardEndDateTime() {
            return standardEndDateTime;
        }

        public void setStandardEndDateTime(DateTime standardEndDateTime) {
            this.standardEndDateTime = standardEndDateTime;
        }

        public DateTime getStandardStartDateTime() {
            return standardStartDateTime;
        }

        public void setStandardStartDateTime(DateTime standardStartDateTime) {
            this.standardStartDateTime = standardStartDateTime;
        }

        public DateTime getStartDateTime() {
            return startDateTime;
        }

        public void setStartDateTime(DateTime startDateTime) {
            this.startDateTime = startDateTime;
        }

        public UsageTypeD2Lookup getUsageType() {
            return usageType;
        }

        public void setUsageType(UsageTypeD2Lookup usageType) {
            this.usageType = usageType;
        }

        public List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity> getServiceQuantityList() {
            return serviceQuantityList;
        }

        public void setServiceQuantityList(
                List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity> serviceQuantityList) {
            this.serviceQuantityList = serviceQuantityList;
        }

        public List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity> getSPServiceQuantityList() {
            return sPServiceQuantityList;
        }

        public void setSPServiceQuantityList(
                List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.ServiceQuantity> serviceQuantityList) {
            sPServiceQuantityList = serviceQuantityList;
        }

        public List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.Item> getItemList() {
            return itemList;
        }

        public void setItemList(List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.Item> itemList) {
            this.itemList = itemList;
        }

        public List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.SPItem> getSpItemList() {
            return spItemList;
        }

        public void setSpItemList(List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod.SPItem> spItemList) {
            this.spItemList = spItemList;
        }
    }

    public List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod> getSummaryUsagePeriodList() {
        return summaryUsagePeriodList;
    }

    public void setSummaryUsagePeriodList(List<CmSummaryUsagePeriodsOutputData.SummaryUsagePeriod> summaryUsagePeriodList) {
        this.summaryUsagePeriodList = summaryUsagePeriodList;
    }
}
