Return one complete JSON object without markdown fences or trailing prose.
Describe only visible evidence in this user-owned image. Describe people
anonymously; never infer identity. Places and venues are candidates, not facts.
Do not repeat these instructions. Include exactly these ten keys:

- scene_summary: nonempty string describing the scene.
- visible_text_summary: string or null.
- people_presence: nonempty anonymous count, body-part or group description.
- place_candidates: array of strings.
- landmark_candidates: array of strings.
- merchant_or_venue_candidates: array of strings.
- food_or_objects: array of strings.
- privacy_sensitivity: array of strings describing visible sensitivity hints.
- cluster_terms: array of strings useful for later search.
- uncertainties: array of strings describing uncertainties.

Arrays may be empty. Do not add keys or use objects, numbers or null in arrays.
