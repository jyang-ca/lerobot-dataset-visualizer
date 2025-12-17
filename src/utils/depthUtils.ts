
/**
 * Processes a float32 depth map for visualization.
 * Converts float32 values to uint8 (0-255) with normalization.
 * 
 * @param data The raw depth data as a numeric array or Float32Array
 * @param shape Optional shape tuple [height, width, channels]
 * @returns Uint8Array containing the normalized pixel values
 */
export function processDepthMap(data: number[] | Float32Array | Float64Array): Uint8Array {
  try {
    if (!data || data.length === 0) {
      throw new Error("Empty depth data provided");
    }

    // 1. Find min and max for normalization
    let min = Infinity;
    let max = -Infinity;
    let hasValidData = false;

    for (let i = 0; i < data.length; i++) {
      const val = data[i];
      // Skip NaNs and Infinities for min/max calculation
      if (Number.isFinite(val)) {
        if (val < min) min = val;
        if (val > max) max = val;
        hasValidData = true;
      }
    }

    if (!hasValidData) {
      // If no valid data, return all zeros
      return new Uint8Array(data.length);
    }

    // 2. Normalize and convert to Uint8
    const result = new Uint8Array(data.length);
    const range = max - min; // 240, 320, 1 -> 

    // Avoid division by zero if all values are the same
    if (range === 0) {
      // If all values are the same, just set to 0 or 128? 
      // Let's set to 0 (black) as per standard depth map visualization for uniform depth
      return result; 
    }

    for (let i = 0; i < data.length; i++) {
      const val = data[i];
      if (!Number.isFinite(val)) {
        result[i] = 0; // Handle NaN/Inf as 0
        continue;
      }
      
      // Normalize to 0-1
      const normalized = (val - min) / range;
      
      // Apply gamma correction to brighten the image (gamma = 0.4)
      const brightened = Math.pow(normalized, 0.4);
      
      // Scale to 180-255 range to eliminate excessive darkness
      // This ensures all depth values are clearly visible
      const byteVal = Math.floor(180 + brightened * 75);
      result[i] = Math.max(180, Math.min(255, byteVal));
    }

    return result;

  } catch (error) {
    console.error("Error processing depth map:", error);
    // Return empty array or zeros on error to prevent crashing data loading
    return new Uint8Array(data ? data.length : 0);
  }
}
