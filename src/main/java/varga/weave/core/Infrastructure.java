package varga.weave.core;

/*-
 * #%L
 * Weave
 * %%
 * Copyright (C) 2025 - 2026 Varga Foundation
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import lombok.Data;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@Data
public class Infrastructure implements Serializable {
    /**
     * A incremental unique identifier.
     */
    private String id;
    /**
     * The infrastructure type, can be: Private, Azure, AWS, GCP, Hetzner...
     */
    private String type;
    /**
     * The infrastructure logical location, ex: Helsinki DC3, GRA1 DC3, Geneva CH-GVA-2...
     */
    private String location;
    /**
     * The infrastructure country location, ex: fr, ch...
     */
    private String country;
    /**
     * The infrastructure latitude
     */
    private BigDecimal latitude;
    /**
     * The infrastructure longitude
     */
    private BigDecimal longitude;
    /**
     * The infrastructure name.
     */
    private String name;
    /**
     * The infrastructure description.
     */
    private String description;

    private Map<String, String> labels = new HashMap<>();
    /**
     * Represent the instant of the creation of this infrastructure.
     */
    private Instant created;
    /**
     * Represent the instant when the infrastructure has been updated.
     */
    /**
     * A raw bucket containing all the specific properties of the infrastructure.
     */
    private Metadata metadata;
}
