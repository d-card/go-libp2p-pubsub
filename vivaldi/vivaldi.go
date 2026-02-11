// Package vivaldi implements the Vivaldi and Newton-Vivaldi coordinate update logic for virtual coordinate systems.
package vivaldi

import (
	"math"
)

// Coord represents a 2D Euclidean coordinate with a height component.
type Coord struct {
	X float64
	Y float64
	H float64 // Height
}

// VivaldiState holds the local coordinate and error estimate.
type VivaldiState struct {
	Coord Coord
	Error float64
}

// VivaldiUpdateParams holds the parameters for a single update step.
type VivaldiUpdateParams struct {
	Local            VivaldiState // Our node's state
	Remote           VivaldiState // State from other peer
	RTT              float64      // Measured RTT (ms)
	Ce               float64      // Local learning rate (e.g. 0.25)
	Newton           bool         // If true, apply Newton-Vivaldi security checks
	TrustScore       float64      // Optional: trust score for remote (0-1, only used if Newton=true). TODO: check Newton paper for good values.
	OutlierThreshold float64      // Optional: threshold for outlier rejection (only used if Newton=true). TODO: check Newton paper for good values.
}

// UpdateVivaldi performs a Vivaldi/Newton-Vivaldi coordinate update step.
// Returns the new local coordinate and error.
func UpdateVivaldi(params VivaldiUpdateParams) (Coord, float64) {
	// Predicted distance
	predDist := euclideanDist(params.Local.Coord, params.Remote.Coord) + params.Local.Coord.H + params.Remote.Coord.H
	// Error
	delta := params.RTT - predDist
	// Combine errors
	totalErr := params.Local.Error + params.Remote.Error
	if totalErr == 0 {
		totalErr = 1e-6 // avoid div by zero
	}
	// Weight for remote
	w := params.Local.Error / totalErr
	if w < 0.01 {
		w = 0.01
	}
	if w > 0.99 {
		w = 0.99
	}

	// Newton checks
	if params.Newton {
		if params.OutlierThreshold > 0 && math.Abs(delta) > params.OutlierThreshold {
			// Outlier: ignore update
			return params.Local.Coord, params.Local.Error
		}
		if params.TrustScore > 0 && params.TrustScore < 1 {
			w = w * params.TrustScore
		}
	}

	// Update coordinate
	adj := params.Ce * w
	newCoord := Coord{
		X: params.Local.Coord.X + adj*(params.Remote.Coord.X-params.Local.Coord.X+delta*unit(params.Local.Coord, params.Remote.Coord).X),
		Y: params.Local.Coord.Y + adj*(params.Remote.Coord.Y-params.Local.Coord.Y+delta*unit(params.Local.Coord, params.Remote.Coord).Y),
		H: params.Local.Coord.H + adj*(params.Remote.Coord.H-params.Local.Coord.H+delta*0.5), // height update simplified
	}

	// Update error
	newErr := params.Local.Error + params.Ce*(math.Abs(delta)-params.Local.Error)
	if newErr < 1e-6 {
		newErr = 1e-6
	}
	return newCoord, newErr
}

func euclideanDist(a, b Coord) float64 {
	dx := a.X - b.X
	dy := a.Y - b.Y
	return math.Sqrt(dx*dx + dy*dy)
}

// unit returns the unit vector from a to b in 2D.
func unit(a, b Coord) Coord {
	dx := b.X - a.X
	dy := b.Y - a.Y
	d := math.Sqrt(dx*dx + dy*dy)
	if d == 0 {
		return Coord{0, 0, 0}
	}
	return Coord{dx / d, dy / d, 0}
}
