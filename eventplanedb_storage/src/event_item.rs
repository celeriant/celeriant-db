use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use crate::serde_array_byte_vec_base64;
use crate::serde_arrays_byte_12_base64;

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct EventItem {
    #[serde(rename = "li")]
    pub local_index: u64,

    #[serde(rename = "ed")]
    pub event_date: u64,

    #[serde(rename = "tp")]
    pub event_type: u64,

    #[serde(skip_serializing_if = "Option::is_none", rename = "vi")]
    pub int_values: Option<Vec<i64>>,

    #[serde(skip_serializing_if = "Option::is_none", rename = "vu")]
    pub uint_values: Option<Vec<u64>>,

    #[serde(skip_serializing_if = "Option::is_none", rename = "vf")]
    pub f32_values: Option<Vec<f32>>,

    #[serde(skip_serializing_if = "Option::is_none", rename = "vd")]
    pub f64_values: Option<Vec<f64>>,

    #[serde(skip_serializing_if = "Option::is_none", rename = "vb")]
    pub bool_values: Option<Vec<bool>>,

    #[serde(skip_serializing_if = "Option::is_none", rename = "sv")]
    pub string_values: Option<Vec<Option<String>>>,

    #[serde(skip_serializing_if = "Option::is_none", rename = "iv", default, with = "serde_arrays_byte_12_base64")]
    pub iv_arrays: Option<Vec<Option<[u8; 12]>>>,

    #[serde(skip_serializing_if = "Option::is_none", rename = "by", default, with = "serde_array_byte_vec_base64")]
    pub byte_arrays: Option<Vec<Option<Vec<u8>>>>,
}

impl EventItem {
    pub fn new() -> Self {
        EventItem {
            local_index: 0,
            event_date: 0,
            event_type: 0,
            int_values: None,
            uint_values: None,
            f32_values: None,
            f64_values: None,
            bool_values: None,
            string_values: None,
            iv_arrays: None,
            byte_arrays: None,
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::fs;
    use tempfile::TempDir;

    use super::*;

    pub fn create_test_event_item() -> EventItem {
        let (stl_f32, stl_i32) = test_stl_data();
        let image_bytes = test_image(0);
        let lorem_text = test_read_lorem();

        let mut event1 = EventItem::new();

        event1.event_date = 443;
        event1.event_type = 4;
        event1.int_values = Some(stl_i32.into_iter().map(|v| v as i64).collect());
        event1.f32_values = Some(stl_f32.into_iter().map(|v| v).collect());
        event1.string_values = Some(lorem_text.into_iter().map(|v| Some(v)).collect());
        event1.byte_arrays = Some(vec![Some(image_bytes)]);

        event1
    }

    pub fn create_minimal_event_item() -> EventItem {
        let mut event2 = EventItem::new();

        event2.uint_values = Some(vec![1, 2, 3]);
        event2.f64_values = Some(vec![1.0, 2.0, 3.0]);
        event2.bool_values = Some(vec![true, false, true]);
        event2.string_values = Some(vec![Some("Hello".to_string()), Some("World".to_string())]);
        event2.event_date = 443;
        event2.event_type = 4;

        event2
    }

    pub fn write_test_stl(vertices: &[f32], indices: &[i32], path: &std::path::Path) {
        use std::fs::OpenOptions;

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .expect("Failed to create STL file");

        // Convert flat vertex array to Vector3 points
        let vertex_points: Vec<[f32; 3]> = vertices.chunks(3).map(|chunk| [chunk[0], chunk[1], chunk[2]]).collect();

        // Create triangles from indices
        let triangles: Vec<stl_io::Triangle> = indices
            .chunks(3)
            .map(|face_indices| {
                let v0 = vertex_points[face_indices[0] as usize];
                let v1 = vertex_points[face_indices[1] as usize];
                let v2 = vertex_points[face_indices[2] as usize];

                // Calculate normal (cross product of two edges)
                let edge1 = [v1[0] - v0[0], v1[1] - v0[1], v1[2] - v0[2]];
                let edge2 = [v2[0] - v0[0], v2[1] - v0[1], v2[2] - v0[2]];
                let normal = [
                    edge1[1] * edge2[2] - edge1[2] * edge2[1],
                    edge1[2] * edge2[0] - edge1[0] * edge2[2],
                    edge1[0] * edge2[1] - edge1[1] * edge2[0],
                ];

                stl_io::Triangle {
                    normal: stl_io::Vector::new(normal),
                    vertices: [stl_io::Vector::new(v0), stl_io::Vector::new(v1), stl_io::Vector::new(v2)],
                }
            })
            .collect();

        stl_io::write_stl(&mut file, triangles.iter()).expect("Failed to write STL file");
    }

    pub fn test_read_lorem() -> Vec<String> {
        let ipsum_text = fs::read_to_string("tests/resources/ipsum.txt").expect("Failed to read tests/resources/ipsum.txt");

        ipsum_text.lines().filter(|line| !line.is_empty()).map(|line| line.to_string()).collect()
    }

    pub fn test_stl_data() -> (Vec<f32>, Vec<i32>) {
        use std::fs::OpenOptions;

        let mut file = OpenOptions::new()
            .read(true)
            .open("tests/resources/mesh.stl")
            .expect("Failed to open tests/resources/mesh.stl");

        let stl = stl_io::read_stl(&mut file).expect("Failed to read STL file");

        // Extract all vertex coordinates into a flat f32 array
        let vertices: Vec<f32> = stl.vertices.iter().flat_map(|vertex| [vertex[0], vertex[1], vertex[2]]).collect();

        // Extract face indices
        let indices: Vec<i32> = stl.faces.iter().flat_map(|face| face.vertices.iter()).map(|&idx| idx as i32).collect();

        (vertices, indices)
    }

    pub fn test_image(blur: u32) -> Vec<u8> {
        let img = image::open("tests/resources/image.jpg").expect("Failed to read tests/resources/image.jpg");

        let processed_img = if blur == 0 { img } else { img.blur(blur as f32) };

        // Encode as JPEG and return the encoded bytes
        let mut buffer = Vec::new();
        processed_img
            .write_to(&mut std::io::Cursor::new(&mut buffer), image::ImageFormat::Jpeg)
            .expect("Failed to encode image");
        buffer
    }

    #[test]
    fn test_write_images() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let temp_path = temp_dir.path();

        let blur_ratio = 3;
        let image_bytes = test_image(blur_ratio);

        let path = temp_path.join(format!("image_blurred_{}.jpg", blur_ratio));
        let img = image::load_from_memory(&image_bytes).expect("Failed to load image");

        img.save(&path).expect("Failed to save image");

        assert!(path.exists());
    }

    #[test]
    fn test_write_lorem() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let temp_path = temp_dir.path();

        let paragraphs = test_read_lorem();

        for (i, paragraph) in paragraphs.iter().enumerate() {
            let path = temp_path.join(format!("lorem_{}.txt", i));
            let uppercase_content = paragraph.to_uppercase();
            std::fs::write(&path, uppercase_content).expect("Failed to write file");
            assert!(path.exists());
        }
    }

    #[test]
    fn test_write_stl() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let temp_path = temp_dir.path();

        let (vertices, faces) = test_stl_data();

        let mut scaled_vertices = vertices.to_vec();
        for i in (2..scaled_vertices.len()).step_by(3) {
            // Every 3rd element starting from index 2 (Z component)
            scaled_vertices[i] *= 1.5;
        }

        let path = temp_path.join("mesh.stl");
        write_test_stl(&scaled_vertices, &faces, &path);

        assert!(path.exists());
    }
}
