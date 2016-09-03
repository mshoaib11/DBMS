/*
 * Copyright(c) 2012 Saarland University - Information Systems Group
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dbs_project.index;

/**
 * Enum for the different possible index types:
 * <p/>
 * HASH : hash based index for point queries (no range support)
 * <p/>
 * TREE : tree based index for point and range queries
 * <p/>
 * BITMAP : bitmap index, as explained in the lecture
 * <p/>
 * OTHER : will never be created by our tests, but gives you a possibility to
 * include a custom index structure.
 */
public enum IndexType {

    HASH, TREE, BITMAP, OTHER
}
