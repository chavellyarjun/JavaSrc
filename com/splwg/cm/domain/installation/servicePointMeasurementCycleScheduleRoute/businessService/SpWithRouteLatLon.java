package com.splwg.cm.domain.installation.servicePointMeasurementCycleScheduleRoute.businessService;

public class SpWithRouteLatLon implements Comparable<SpWithRouteLatLon>{
	
	private String spId;
	private String route;
	private Double lat;
	private Double lon;
	private Double distance;
	
	public SpWithRouteLatLon(String spId, String route, Double lat, Double lon) {
		super();
		this.spId = spId;
		this.route = route;
		this.lat = lat;
		this.lon = lon;
	}
	public String getSpId() {
		return spId;
	}
	public void setSpId(String spId) {
		this.spId = spId;
	}
	public String getRoute() {
		return route;
	}
	public void setRoute(String route) {
		this.route = route;
	}
	public Double getLat() {
		return lat;
	}
	public void setLat(Double lat) {
		this.lat = lat;
	}
	public Double getLon() {
		return lon;
	}
	public void setLon(Double lon) {
		this.lon = lon;
	}
	public Double getDistance() {
		return distance;
	}
	public void setDistance(Double distance) {
		this.distance = distance;
	}
	@Override
	public int compareTo(SpWithRouteLatLon o) {
		if(distance == o.getDistance())  
			return 0;  
		else if(distance > o.getDistance())  
			return 1;  
		else  
			return -1; 
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((spId == null) ? 0 : spId.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SpWithRouteLatLon other = (SpWithRouteLatLon) obj;
		if (spId == null) {
			if (other.spId != null)
				return false;
		} else if (!spId.equals(other.spId))
			return false;
		return true;
	}
	@Override
	public String toString() {
		return "SpWithRouteLatLon [spId=" + spId + ", route=" + route
				+ ", lat=" + lat + ", lon=" + lon + ", distance=" + distance
				+ "]";
	}
}
