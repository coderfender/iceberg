/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class SnapshotLineage {

    public SnapshotLineage() throws Exception { throw new Exception("Not Implemented!");}

    public static List<Snapshot> getShortestSnapshotLineage(Table table, Snapshot s1, Snapshot s2) {
        List<Snapshot> pathToRootSnapshot1 = getPathToRoot(table, s1);
        List<Snapshot> pathToRootSnapshot2 = getPathToRoot(table, s2);
        Snapshot lowestCommonAncestor = findLowestCommonAncestor(pathToRootSnapshot1, pathToRootSnapshot2);
        List<Snapshot> shortestPath = new ArrayList<>();
        for (int i = 0; i < pathToRootSnapshot1.size() && pathToRootSnapshot2.get(i) != lowestCommonAncestor; i++) {
            shortestPath.add(pathToRootSnapshot1.get(i));
        }

        Collections.reverse(shortestPath);
        shortestPath.add(lowestCommonAncestor);

        for (int i = 0; i < pathToRootSnapshot2.size() && pathToRootSnapshot2.get(i) != lowestCommonAncestor; i++) {
            shortestPath.add(pathToRootSnapshot2.get(i));
        }

        return shortestPath;
    }

    public static List<Snapshot> getPathToRoot(Table table, Snapshot snapshot){
        List<Snapshot> result = Lists.newArrayList();
            while (snapshot != null){
                result.add(snapshot);
                if (snapshot.parentId() != null){
                    snapshot = table.snapshot(snapshot.parentId());
                }
                else {
                    break;
                }
            }
        return result;
    }

    public static Snapshot findLowestCommonAncestor(List<Snapshot> pathToRoot1, List<Snapshot> pathToRoot2){
        HashSet<Snapshot> pathToRoot1Set = new HashSet<>(pathToRoot1);
        for (Snapshot snapshot:pathToRoot2){
            if (pathToRoot1Set.contains(snapshot)){
                return snapshot;
            }
        }
        return null;
    }
}
