/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 */

package com.idvp.data.infrastructure.scheduling.quarz.store.hazelcast.wrappers;

import com.hazelcast.map.IMap;
import com.idvp.data.infrastructure.scheduling.quarz.store.hazelcast.collections.InstanceHolder;
import org.quartz.JobKey;

import java.util.Set;

@SuppressWarnings("UnusedReturnValue")
public class JobFacade {
    private final IMap<JobKey, JobWrapper> jobsByFQN;
    private final Set<String> allJobsGroupNames;
    private final Set<String> pausedJobGroups;
    private final Set<JobKey> blockedJobs;

    public JobFacade(final InstanceHolder toolkitDSHolder) {
        this.jobsByFQN = toolkitDSHolder.getOrCreateJobsMap();
        this.allJobsGroupNames = toolkitDSHolder.getOrCreateAllGroupsSet();
        this.pausedJobGroups = toolkitDSHolder.getOrCreatePausedGroupsSet();
        this.blockedJobs = toolkitDSHolder.getOrCreateBlockedJobsSet();
    }

    public JobWrapper get(JobKey jobKey) {
        return jobsByFQN.get(jobKey);
    }

    public void put(JobKey jobKey, JobWrapper jobWrapper) {
        jobsByFQN.put(jobKey, jobWrapper);
    }

    public boolean containsKey(JobKey key) {
        return jobsByFQN.containsKey(key);
    }

    public boolean hasGroup(String name) {
        return allJobsGroupNames.contains(name);
    }

    public boolean addGroup(String name) {
        return allJobsGroupNames.add(name);
    }

    public boolean addPausedGroup(String name) {
        return pausedJobGroups.add(name);
    }

    public JobWrapper remove(JobKey jobKey) {
        return jobsByFQN.remove(jobKey);
    }

    public boolean removeGroup(String group) {
        return allJobsGroupNames.remove(group);
    }

    public boolean pausedGroupsContain(String group) {
        return pausedJobGroups.contains(group);
    }

    public boolean blockedJobsContain(JobKey jobKey) {
        return blockedJobs.contains(jobKey);
    }

    public int numberOfJobs() {
        return jobsByFQN.size();
    }

    public Set<String> getAllGroupNames() {
        return allJobsGroupNames;
    }

    public boolean removePausedJobGroup(String group) {
        return pausedJobGroups.remove(group);
    }

    public void clearPausedJobGroups() {
        pausedJobGroups.clear();
    }

    public void addBlockedJob(JobKey key) {
        blockedJobs.add(key);
    }

    public boolean removeBlockedJob(JobKey key) {
        return blockedJobs.remove(key);
    }
}