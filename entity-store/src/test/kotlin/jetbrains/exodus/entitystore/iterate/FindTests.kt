/**
 * Copyright 2010 - 2022 JetBrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jetbrains.exodus.entitystore.iterate

import jetbrains.exodus.TestFor
import jetbrains.exodus.bindings.ComparableSet
import jetbrains.exodus.entitystore.*
import jetbrains.exodus.kotlin.notNull
import jetbrains.exodus.util.Random
import org.junit.Assert

class FindTests : EntityStoreTestBase() {

    fun testFindSingleEntityByPropertyValue() {
        val txn = storeTransactionSafe
        for (i in 0..99) {
            val entity = txn.newEntity("Issue")
            entity.setProperty("description", "Test issue #$i")
            entity.setProperty("size", i)
        }
        txn.flush()
        // find issues with size 50
        val issues = txn.find("Issue", "size", 50)
        var count = 0
        for (issue in issues) {
            Assert.assertEquals(50, issue.getProperty("size"))
            count++
        }
        Assert.assertEquals(1, count.toLong())
    }

    fun testFindByStringPropertyValue() {
        val txn = storeTransactionSafe
        for (i in 0..99) {
            val entity = txn.newEntity("Issue")
            entity.setProperty("description", "Test issue #" + i % 10)
            entity.setProperty("size", i)
        }
        txn.flush()
        val issues = txn.find("Issue", "description", "Test issue #5")
        var count = 0
        for (issue in issues) {
            Assert.assertEquals("Test issue #5", issue.getProperty("description"))
            count++
        }
        Assert.assertEquals(10, count.toLong())
    }

    fun testFindByStringPropertyValueIgnoreCase() {
        val txn = storeTransactionSafe
        for (i in 0..99) {
            val entity = txn.newEntity("Issue")
            entity.setProperty("description", "Test issue #" + i % 10)
        }
        txn.flush()
        val issues = txn.find("Issue", "description", "Test ISSUE #5")
        var count = 0
        for (issue in issues) {
            Assert.assertEquals("Test issue #5", issue.getProperty("description"))
            count++
        }
        Assert.assertEquals(10, count.toLong())
    }

    @TestFor(issue = "XD-824")
    fun testFindContaining() {
        val txn = storeTransactionSafe
        for (i in 0..99) {
            val entity = txn.newEntity("Issue")
            entity.setProperty("description", "Test issue #" + i % 10)
        }
        txn.flush()
        val issues = txn.findContaining("Issue", "description", "e #5", false)
        var count = 0
        for (issue in issues) {
            Assert.assertEquals("Test issue #5", issue.getProperty("description"))
            count++
        }
        Assert.assertEquals(10, count.toLong())
    }

    @TestFor(issue = "XD-824")
    fun testFindContainingIgnoreCase() {
        val txn = storeTransactionSafe
        for (i in 0..99) {
            val entity = txn.newEntity("Issue")
            entity.setProperty("description", "Test issue #" + i % 10)
        }
        txn.flush()
        val issues = txn.findContaining("Issue", "description", "T ISSUE #5", true)
        var count = 0
        for (issue in issues) {
            Assert.assertEquals("Test issue #5", issue.getProperty("description"))
            count++
        }
        Assert.assertEquals(10, count.toLong())
    }

    @TestFor(issue = "XD-837")
    fun testFindContainingIntersect() {
        val txn = storeTransactionSafe
        val project = txn.newEntity("Project")
        for (i in 0..99) {
            val issue = txn.newEntity("Issue")
            issue.setProperty("description", ((100 - i) % 9).toString() + "Test issue #" + i % 10)
            project.addLink("issues", issue)
        }
        txn.flush()
        val issues =
            txn.findContaining("Issue", "description", "T ISSUE #5", true).intersect(project.getLinks("issues"))
        var count = 0
        for (issue in issues) {
            Assert.assertEquals("Test issue #5", (issue.getProperty("description") as String?).notNull.substring(1))
            count++
        }
        Assert.assertEquals(10, count.toLong())
    }

    fun testSingularFind() {
        val txn = storeTransactionSafe
        val entity = txn.newEntity("Issue")
        entity.setProperty("name", "noname")
        entity.setProperty("size", 6)
        txn.flush()
        Assert.assertEquals(0, txn.find("Issue", "name", "thename").size().toInt().toLong())
        Assert.assertEquals(0, txn.find("Issue", "name", 6).size().toInt().toLong())
        Assert.assertEquals(0, txn.find("Issue", "size", "wtf").size().toInt().toLong())
        Assert.assertEquals(0, txn.find("Issue", "description", "Test Issue").size().toInt().toLong())
    }

    fun testFindByPropAfterSeveralTxns() {
        val pi = 31415926
        val e = 271828182
        val txn = storeTransactionSafe
        var issue: Entity = txn.newEntity("Issue")
        issue.setProperty("size", pi)
        txn.flush()
        Assert.assertEquals(1, txn.find("Issue", "size", pi).size().toInt().toLong())
        issue = txn.newEntity("Issue")
        issue.setProperty("size", pi)
        txn.flush()
        Assert.assertEquals(2, txn.find("Issue", "size", pi).size().toInt().toLong())
        issue = txn.newEntity("Issue")
        issue.setProperty("size", pi)
        txn.flush()
        Assert.assertEquals(3, txn.find("Issue", "size", pi).size().toInt().toLong())
        issue = txn.newEntity("Issue")
        issue.setProperty("size", e)
        txn.flush()
        Assert.assertEquals(3, txn.find("Issue", "size", pi).size().toInt().toLong())
        Assert.assertEquals(1, txn.find("Issue", "size", e).size().toInt().toLong())
        issue = txn.find("Issue", "size", pi).iterator().next()
        issue.setProperty("size", e)
        txn.flush()
        Assert.assertEquals(2, txn.find("Issue", "size", pi).size().toInt().toLong())
        Assert.assertEquals(2, txn.find("Issue", "size", e).size().toInt().toLong())
        issue = txn.find("Issue", "size", pi).iterator().next()
        issue.setProperty("size", e)
        txn.flush()
        Assert.assertEquals(1, txn.find("Issue", "size", pi).size().toInt().toLong())
        Assert.assertEquals(txn.find("Issue", "size", e).size().toInt().toLong(), 3)
        issue = txn.find("Issue", "size", pi).iterator().next()
        issue.setProperty("size", e)
        txn.flush()
        Assert.assertEquals(0, txn.find("Issue", "size", pi).size().toInt().toLong())
        Assert.assertEquals(4, txn.find("Issue", "size", e).size().toInt().toLong())
    }

    fun testFindByPropAfterSeveralTxns2() {
        val pi = 31415926
        val e = 271828182
        val txn = storeTransactionSafe
        val issue1 = txn.newEntity("Issue")
        val issue2 = txn.newEntity("Issue")
        issue1.setProperty("size", pi)
        issue2.setProperty("size", e)
        txn.flush()
        Assert.assertEquals(0, txn.find("Issue", "size", 0).size().toInt().toLong())
        Assert.assertEquals(1, txn.find("Issue", "size", pi).size().toInt().toLong())
        Assert.assertEquals(1, txn.find("Issue", "size", e).size().toInt().toLong())
        for (i in 0..100) {
            val t = if (System.currentTimeMillis() and 1 == 1L) pi else e
            issue1.setProperty("size", t)
            issue2.setProperty("size", if (t == pi) e else pi)
            txn.flush()
            Assert.assertEquals(0, txn.find("Issue", "size", 0).size().toInt().toLong())
            Assert.assertEquals(1, txn.find("Issue", "size", pi).size().toInt().toLong())
            Assert.assertEquals(1, txn.find("Issue", "size", e).size().toInt().toLong())
        }
    }

    fun testCreateCheckSize() {
        val txn = storeTransactionSafe
        for (i in 0..999) {
            Assert.assertEquals("Iteration $i", i.toLong(), txn.getAll("Issue").size())
            txn.newEntity("Issue")
            txn.flush()
            Assert.assertEquals("Iteration $i", (i + 1).toLong(), txn.getAll("Issue").size())
        }
    }

    fun testCreateFindByPropValue() {
        val txn = storeTransactionSafe
        var e = txn.newEntity("Issue")
        e.setProperty("size", "S" + 0)
        txn.flush()
        for (i in 0..999) {
            if (!txn.find("Issue", "size", "s$i").iterator().hasNext()) {
                e = txn.newEntity("Issue")
                e.setProperty("size", "S$i")
                txn.flush()
            }
            val it = txn.find("Issue", "size", "s$i")
            if (!it.iterator().hasNext()) {
                throw RuntimeException("Iteration $i, it $it")
            }
        }
    }

    fun testCreateFindByPropValueReverseOrder() {
        val txn = storeTransactionSafe
        var e = txn.newEntity("Issue")
        e.setProperty("size", "S" + 0)
        txn.flush()
        for (i in 1000 downTo 0) {
            if (!txn.find("Issue", "size", "s$i").iterator().hasNext()) {
                e = txn.newEntity("Issue")
                e.setProperty("size", "S$i")
                txn.flush()
            }
            val it = txn.find("Issue", "size", "s$i")
            if (!it.iterator().hasNext()) {
                throw RuntimeException("Iteration $i, it $it")
            }
        }
    }

    fun testCreateFindByPropValueAndIntersect() {
        val txn = storeTransactionSafe
        var e = txn.newEntity("Issue")
        val owner = txn.newEntity("User")
        e.setProperty("size", 0.toString())
        e.addLink("owner", owner)
        txn.flush()
        for (i in 0..999) {
            if (i % 1000 == 0) {
                println("Iteration: $i")
            }
            var it = txn.find("Issue", "size", i.toString()).intersect(txn.findLinks("Issue", owner, "owner"))
            if (!it.iterator().hasNext()) {
                e = txn.newEntity("Issue")
                e.setProperty("size", i.toString())
                e.addLink("owner", owner)
                txn.flush()
            }
            val links = txn.findLinks("Issue", owner, "owner")
            it = txn.find("Issue", "size", i.toString()).intersect(links)
            val itt = it.iterator()
            if (!itt.hasNext()) {
                throw RuntimeException("0: Iteration $i, it $it, links $links")
            }
            Assert.assertEquals(e, itt.next())
            if (itt.hasNext()) {
                throw RuntimeException("2: Iteration $i, it $it, links $links")
            }
        }
    }

    fun testCreateFindByPropValueAndIntersectReverseOrder() {
        val txn = storeTransactionSafe
        var e = txn.newEntity("Issue")
        val eSaved = e
        val owner = txn.newEntity("User")
        e.setProperty("size", 0.toString())
        e.addLink("owner", owner)
        txn.flush()
        for (i in 1000 downTo 0) {
            var it = txn.find("Issue", "size", i.toString()).intersect(txn.findLinks("Issue", owner, "owner"))
            if (!it.iterator().hasNext()) {
                e = txn.newEntity("Issue")
                e.setProperty("size", i.toString())
                e.addLink("owner", owner)
                txn.flush()
            } else {
                e = eSaved
            }
            val links = txn.findLinks("Issue", owner, "owner")
            it = txn.find("Issue", "size", i.toString()).intersect(links)
            val itt = it.iterator()
            if (!itt.hasNext()) {
                throw RuntimeException("0: Iteration $i, it $it, links $links")
            }
            Assert.assertEquals(e, itt.next())
            if (itt.hasNext()) {
                throw RuntimeException("2: Iteration $i, it $it, links $links")
            }
        }
    }

    fun testOrderByEntityId() {
        val txn = storeTransactionSafe
        val ids: MutableList<EntityId> = ArrayList()
        for (i in 0..9) {
            ids.add(txn.newEntity("Issue").id)
        }
        txn.flush()
        // reverse the order of created ids
        var i = 0
        var j = ids.size - 1
        while (i < j) {
            val id = ids[i]
            ids[i] = ids[j]
            ids[j] = id
            ++i
            --j
        }
        for (id in ids) {
            val e = txn.getEntity(id)
            e.setProperty("description", "Test issue")
            txn.flush()
        }
        val issues = txn.find("Issue", "description", "Test issue")
        var localId: Long = 0
        for (issue in issues) {
            Assert.assertEquals(localId, issue.id.localId) // correct order
            ++localId
        }
        Assert.assertEquals(ids.size.toLong(), localId.toInt().toLong())
    }

    fun testFindRange() {
        val txn = storeTransactionSafe
        for (i in 0..99) {
            val entity = txn.newEntity("Issue")
            entity.setProperty("description", "Test issue #" + i % 10)
            entity.setProperty("size", i)
        }
        txn.flush()
        Assert.assertEquals(0, txn.find("Issue", "size", 101, 102).size().toInt().toLong())
        Assert.assertEquals(0, txn.find("Issue", "size", -2, -1).size().toInt().toLong())
        Assert.assertEquals(100, txn.find("Issue", "size", 0, 100).size().toInt().toLong())
        Assert.assertEquals(100, txn.find("Issue", "size", 0, 102).size().toInt().toLong())
        Assert.assertEquals(11, txn.find("Issue", "size", 0, 10).size().toInt().toLong())
        Assert.assertEquals(10, txn.find("Issue", "size", 90, 100).size().toInt().toLong())
        Assert.assertEquals(10, txn.find("Issue", "size", 90, 102).size().toInt().toLong())
    }

    fun testFindRangeReversed() {
        val txn = storeTransactionSafe
        for (i in 0..99) {
            val entity = txn.newEntity("Issue")
            entity.setProperty("description", "Test issue #" + i % 10)
            entity.setProperty("size", i)
        }
        txn.flush()
        Assert.assertEquals(0, txn.find("Issue", "size", 101, 102).reverse().toList().size)
        Assert.assertEquals(0, txn.find("Issue", "size", -2, -1).reverse().toList().size)
        Assert.assertEquals(100, txn.find("Issue", "size", 0, 100).reverse().toList().size)
        Assert.assertEquals(100, txn.find("Issue", "size", 0, 102).reverse().toList().size)
        Assert.assertEquals(11, txn.find("Issue", "size", 0, 10).reverse().toList().size)
        Assert.assertEquals(10, txn.find("Issue", "size", 90, 100).reverse().toList().size)
        Assert.assertEquals(10, txn.find("Issue", "size", 90, 102).reverse().toList().size)
    }

    fun testFindRangeOrder() {
        val txn = storeTransactionSafe
        for (i in 0..99) {
            val issue = txn.newEntity("Issue")
            issue.setProperty("description", "Test issue #" + i % 10)
            issue.setProperty("size", 99 - i)
        }
        txn.flush()
        for ((i, issue) in txn.find("Issue", "size", 0, 100).withIndex()) {
            Assert.assertEquals(i, issue.getProperty("size"))
        }
    }

    fun testFindRangeByStrings() {
        val txn = storeTransactionSafe
        var entity = txn.newEntity("Issue")
        entity.setProperty("description", "aaa")
        entity = txn.newEntity("Issue")
        entity.setProperty("description", "bbb")
        entity = txn.newEntity("Issue")
        entity.setProperty("description", "ccc")
        entity = txn.newEntity("Issue")
        entity.setProperty("description", "dddd")
        txn.flush()
        Assert.assertEquals(4, txn.find("Issue", "description", "a", "e").size().toInt().toLong())
        Assert.assertEquals(3, txn.find("Issue", "description", "b", "e").size().toInt().toLong())
        Assert.assertEquals(3, txn.find("Issue", "description", "a", "d").size().toInt().toLong())
        Assert.assertEquals(2, txn.find("Issue", "description", "a", "c").size().toInt().toLong())
        Assert.assertEquals(2, txn.find("Issue", "description", "b", "d").size().toInt().toLong())
        Assert.assertEquals(2, txn.find("Issue", "description", "c", "e").size().toInt().toLong())
        Assert.assertEquals(1, txn.find("Issue", "description", "a", "b").size().toInt().toLong())
        Assert.assertEquals(1, txn.find("Issue", "description", "d", "e").size().toInt().toLong())
        Assert.assertEquals(0, txn.find("Issue", "description", "a", "a").size().toInt().toLong())
    }

    fun testFindRangeByStringsIgnoreCase() {
        val txn = storeTransactionSafe
        var entity = txn.newEntity("Issue")
        entity.setProperty("description", "aaa")
        entity = txn.newEntity("Issue")
        entity.setProperty("description", "bbb")
        entity = txn.newEntity("Issue")
        entity.setProperty("description", "ccc")
        entity = txn.newEntity("Issue")
        entity.setProperty("description", "dddd")
        txn.flush()
        Assert.assertEquals(4, txn.find("Issue", "description", "a", "E").size().toInt().toLong())
        Assert.assertEquals(3, txn.find("Issue", "description", "B", "e").size().toInt().toLong())
        Assert.assertEquals(3, txn.find("Issue", "description", "a", "D").size().toInt().toLong())
        Assert.assertEquals(2, txn.find("Issue", "description", "A", "c").size().toInt().toLong())
        Assert.assertEquals(2, txn.find("Issue", "description", "B", "D").size().toInt().toLong())
        Assert.assertEquals(2, txn.find("Issue", "description", "C", "E").size().toInt().toLong())
        Assert.assertEquals(1, txn.find("Issue", "description", "A", "B").size().toInt().toLong())
        Assert.assertEquals(1, txn.find("Issue", "description", "D", "E").size().toInt().toLong())
        Assert.assertEquals(0, txn.find("Issue", "description", "a", "A").size().toInt().toLong())
    }

    fun testFindRangeAddedInBackOrder() {
        val txn = storeTransactionSafe
        for (i in 0..99) {
            val entity = txn.newEntity("Issue")
            entity.setProperty("description", "Test issue #" + i % 10)
            entity.setProperty("size", 99 - i)
        }
        txn.flush()
        Assert.assertEquals(100, txn.find("Issue", "size", 0, 100).size().toInt().toLong())
        Assert.assertEquals(11, txn.find("Issue", "size", 0, 10).size().toInt().toLong())
        Assert.assertEquals(10, txn.find("Issue", "size", 90, 100).size().toInt().toLong())
    }

    fun testSingularFindRangeByStrings() {
        val txn = storeTransactionSafe
        val entity = txn.newEntity("Issue")
        entity.setProperty("description", "a")
        txn.flush()
        Assert.assertEquals(0, txn.find("Issue", "description", "e", "a").size().toInt().toLong())
        Assert.assertEquals(1, txn.find("Issue", "description", "a", "a").size().toInt().toLong())
        Assert.assertEquals(0, txn.find("Issue", "description", "E", "A").size().toInt().toLong())
        Assert.assertEquals(1, txn.find("Issue", "description", "A", "A").size().toInt().toLong())
        Assert.assertEquals(1, txn.find("Issue", "description", "a", "A").size().toInt().toLong())
        Assert.assertEquals(1, txn.find("Issue", "description", "A", "a").size().toInt().toLong())
        Assert.assertEquals(0, txn.find("Issue", "size", Int.MIN_VALUE, Int.MAX_VALUE).size().toInt().toLong())
    }

    fun testStartsWith() {
        val txn = storeTransactionSafe
        for (i in 0..99) {
            val entity = txn.newEntity("Issue")
            entity.setProperty("description", "Test issue #$i")
        }
        txn.flush()
        Assert.assertEquals(0, txn.findStartingWith("Issue", "description", "a").size().toInt().toLong())
        Assert.assertEquals(11, txn.findStartingWith("Issue", "description", "Test issue #1").size().toInt().toLong())
        Assert.assertEquals(11, txn.findStartingWith("Issue", "description", "Test issue #5").size().toInt().toLong())
        Assert.assertEquals(11, txn.findStartingWith("Issue", "description", "Test issue #9").size().toInt().toLong())
        Assert.assertEquals(100, txn.findStartingWith("Issue", "description", "Test issue #").size().toInt().toLong())
        Assert.assertEquals(100, txn.findStartingWith("Issue", "description", "Test").size().toInt().toLong())
    }

    fun testFindWithProp() {
        testFindSingleEntityByPropertyValue()
        val txn = storeTransactionSafe
        Assert.assertEquals(100, txn.findWithProp("Issue", "description").size())
        Assert.assertEquals(100, txn.findWithProp("Issue", "size").size())
        Assert.assertEquals(0, txn.findWithProp("Issue", "no such property").size())
        Assert.assertEquals(0, txn.findWithProp("No such type", "size").size())
    }

    fun testFindWithFloatProp() {
        val txn = storeTransactionSafe
        val issue1 = txn.newEntity("Issue")
        issue1.setProperty("thefloat", 12f)
        val issue2 = txn.newEntity("Issue")
        issue2.setProperty("thefloat", 10f)
        val itr = txn.findWithPropSortedByValue("Issue", "thefloat").iterator()
        assertTrue(itr.hasNext())
        assertEquals(issue2, itr.next())
        assertTrue(itr.hasNext())
        assertEquals(issue1, itr.next())
        assertFalse(itr.hasNext())
    }

    @TestFor(issue = "XD-805")
    fun testFindWithNegativeFloatProp() {
        val txn = storeTransactionSafe
        val issue1 = txn.newEntity("Issue")
        issue1.setProperty("thefloat", 12f)
        val issue2 = txn.newEntity("Issue")
        issue2.setProperty("thefloat", -10f)
        val itr = txn.findWithPropSortedByValue("Issue", "thefloat").iterator()
        assertTrue(itr.hasNext())
        assertEquals(issue2, itr.next())
        assertTrue(itr.hasNext())
        assertEquals(issue1, itr.next())
        assertFalse(itr.hasNext())
    }

    fun testFindWithPropSorted() {
        testFindSingleEntityByPropertyValue()
        val txn = storeTransactionSafe
        Assert.assertEquals(100, txn.findWithPropSortedByValue("Issue", "description").size())
        val nonExistent = PersistentEntity(entityStore, PersistentEntityId(111, 0))
        Assert.assertEquals(-1, txn.findWithPropSortedByValue("Issue", "description").indexOf(nonExistent).toLong())
        Assert.assertEquals(100, txn.findWithPropSortedByValue("Issue", "size").size())
        Assert.assertEquals(0, txn.findWithPropSortedByValue("Issue", "no such property").size())
        Assert.assertEquals(0, txn.findWithPropSortedByValue("No such type", "size").size())
    }

    fun testFindWithPropIsCached() {
        entityStore.config.isCachingDisabled = false
        testFindWithProp()
        val txn = storeTransactionSafe
        Assert.assertTrue(
            (txn.findWithProp("Issue", "description").iterator() as EntityIteratorBase).iterable.isCachedInstance
        )
    }

    fun testFindWithPropSortedIsCached() {
        entityStore.config.isCachingDisabled = false
        testFindWithPropSorted()
        val txn = storeTransactionSafe
        Assert.assertTrue(
            (txn.findWithPropSortedByValue("Issue", "description")
                .iterator() as EntityIteratorBase).iterable.isCachedInstance
        )
    }

    @TestFor(issue = "XD-524")
    fun testFindWithPropAndIntersectIsCached() {
        testFindWithPropSortedIsCached()
        val txn = storeTransactionSafe
        var withDescription = txn.findWithPropSortedByValue("Issue", "description")
        withDescription = entityStore.entityIterableCache.putIfNotCached(withDescription)
        Assert.assertTrue((withDescription.iterator() as EntityIteratorBase).iterable.isCachedInstance)
        val intersect = withDescription.intersect(txn.findWithProp("Issue", "size")) as EntityIterableBase
        Assert.assertTrue(intersect.canBeCached())
        Assert.assertEquals(100, intersect.size())
        Assert.assertTrue((intersect.iterator() as EntityIteratorBase).iterable.isCachedInstance)
    }

    @TestFor(issue = "XD-524")
    fun testFindWithPropAndUnionIsCached() {
        testFindWithPropSortedIsCached()
        val txn = storeTransactionSafe
        var withDescription = txn.findWithPropSortedByValue("Issue", "description")
        withDescription = entityStore.entityIterableCache.putIfNotCached(withDescription)
        Assert.assertTrue((withDescription.iterator() as EntityIteratorBase).iterable.isCachedInstance)
        val union = withDescription.union(txn.findWithProp("Issue", "size")) as EntityIterableBase
        Assert.assertTrue(union.canBeCached())
        Assert.assertEquals(100, union.size())
        Assert.assertTrue((union.iterator() as EntityIteratorBase).iterable.isCachedInstance)
    }

    @TestFor(issue = "XD-524")
    fun testFindWithPropAndMinusIsCached() {
        testFindWithPropSortedIsCached()
        val txn = storeTransactionSafe
        var withDescription = txn.findWithPropSortedByValue("Issue", "description")
        withDescription = entityStore.entityIterableCache.putIfNotCached(withDescription)
        Assert.assertTrue((withDescription.iterator() as EntityIteratorBase).iterable.isCachedInstance)
        txn.getAll("Issue").first.notNull.setProperty("created", System.currentTimeMillis())
        txn.flush()
        val minus = withDescription.minus(txn.findWithProp("Issue", "created")) as EntityIterableBase
        Assert.assertTrue(minus.canBeCached())
        Assert.assertEquals(99, minus.size())
        Assert.assertTrue((minus.iterator() as EntityIteratorBase).iterable.isCachedInstance)
    }

    fun testFindWithBlob() {
        val txn = storeTransactionSafe
        for (i in 0..99) {
            val entity = txn.newEntity("Issue")
            entity.setBlobString("description", "Test issue #$i")
            entity.setProperty("size", i)
        }
        txn.flush()
        Assert.assertEquals(100, txn.findWithBlob("Issue", "description").size())
        Assert.assertEquals(0, txn.findWithBlob("Issue", "no such blob").size())
    }

    fun testFindComparableSet() {
        val txn = storeTransactionSafe
        val issue = txn.newEntity("Issue")
        val randomSet = ComparableSet<Int?>()
        val rnd = Random()
        val setSize = 20
        for (i in 0 until setSize) {
            randomSet.addItem(rnd.nextInt())
        }
        for (i in 0..999) {
            Assert.assertEquals(setSize.toLong(), randomSet.size().toLong())
            issue.setProperty("randomSet", randomSet)
            randomSet.forEach { item: Int?, index: Int ->
                Assert.assertEquals(
                    issue, txn.find(
                        "Issue", "randomSet",
                        item!!
                    ).first
                )
            }
            Assert.assertTrue(randomSet.removeItem(randomSet.iterator().next()!!))
            while (true) {
                val newItem = rnd.nextInt()
                if (randomSet.addItem(newItem)) {
                    Assert.assertTrue(txn.find("Issue", "randomSet", newItem).isEmpty)
                    break
                }
            }
            if (i % 20 == 19) {
                txn.flush()
            }
        }
    }

    @TestFor(issue = "XD-510")
    @Throws(InterruptedException::class)
    fun testFindComparableSetRange() {
        val txn = storeTransactionSafe
        val issue = txn.newEntity("Issue")
        val set = ComparableSet<String?>()
        set.addItem("Eugene")
        issue.setProperty("commenters", set)
        txn.flush()
        for (i in 0..19) {
            Assert.assertEquals(issue, txn.findStartingWith("Issue", "commenters", "eug").first)
            set.addItem("" + i)
            issue.setProperty("commenters", set)
            txn.flush()
            Thread.sleep(20)
        }
    }

    @TestFor(issue = "XD-511")
    fun testFindComparableSetCaseInsensitive() {
        val txn = storeTransactionSafe
        val issue = txn.newEntity("Issue")
        val set = ComparableSet<String?>()
        set.addItem("Eugene")
        set.addItem("MAX")
        set.addItem("SlaVa")
        set.addItem("Pavel")
        set.addItem("AnnA")
        issue.setProperty("commenters", set)
        txn.flush()
        Assert.assertEquals(issue, txn.find("Issue", "commenters", "eugene").first)
        Assert.assertEquals(issue, txn.find("Issue", "commenters", "Max").first)
        Assert.assertEquals(issue, txn.find("Issue", "commenters", "slaVa").first)
        Assert.assertEquals(issue, txn.findStartingWith("Issue", "commenters", "Pav").first)
        Assert.assertEquals(issue, txn.findStartingWith("Issue", "commenters", "ann").first)
    }

    @TestFor(issue = "XD-512")
    fun testComparableSetPropertiesIterable() {
        entityStore.config.isCachingDisabled = true // disable caching to avoid background exceptions
        testFindComparableSetCaseInsensitive()
        val txn = storeTransactionSafe
        Assert.assertTrue(txn.findWithPropSortedByValue("Issue", "commenters").iterator().hasNext())
    }

    @TestFor(issue = "XD-577")
    fun testSuccessiveInvalidationAndUpdateCachedResult() {
        val txn = storeTransactionSafe
        val issue = txn.newEntity("Issue")
        issue.setProperty("summary", "summary")
        Assert.assertEquals(1L, txn.findStartingWith("Issue", "summary", "summary").size())
        issue.setProperty("summary", "no summary")
        Assert.assertEquals(0L, txn.findStartingWith("Issue", "summary", "summary").size())
        issue.setProperty("summary", "summary")
        Assert.assertEquals(1L, txn.findStartingWith("Issue", "summary", "summary").size())
    }

    @TestFor(issue = "XD-618")
    fun testInvalidateComparableSetPropertyIterables() {
        testFindComparableSetCaseInsensitive()
        try {
            Thread.sleep(1000)
        } catch (ignore: InterruptedException) {
        }
        val txn = storeTransactionSafe
        val issue = txn.getAll("Issue").first.notNull
        val set = issue.getProperty("commenters") as ComparableSet<String>
        set.notNull.addItem("bot")
        issue.setProperty("commenters", set)
    }

    @TestFor(issue = "XD-845")
    fun testSearchByFalse() {
        val txn = storeTransactionSafe
        val deletedIssue: Entity = txn.newEntity("Issue")
        deletedIssue.setProperty("deleted", true)
        val notDeletedIssue: Entity = txn.newEntity("Issue")
        notDeletedIssue.setProperty("deleted", false)
        Assert.assertEquals(1L, txn.findWithPropSortedByValue("Issue", "deleted").size())
        Assert.assertEquals(1L, txn.getAll("Issue").minus(txn.findWithProp("Issue", "deleted")).size())
    }
}
