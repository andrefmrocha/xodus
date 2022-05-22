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

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.ComparableSet
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.entitystore.*
import jetbrains.exodus.entitystore.tables.PropertyTypes
import jetbrains.exodus.env.Cursor

class PropertyRangeIterable(
    txn: PersistentStoreTransaction,
    entityTypeId: Int,
    propertyId: Int,
    minValue: Comparable<*>,
    maxValue: Comparable<*>
) : PropertyRangeOrValueIterableBase(txn, entityTypeId, propertyId) {

    private val min = requireNotNull(PropertyTypes.toLowerCase(minValue))
    private val max = requireNotNull(PropertyTypes.toLowerCase(maxValue))

    override fun isSortedById() = false

    override fun canBeReordered() = true

    override fun getIteratorImpl(txn: PersistentStoreTransaction): EntityIteratorBase {
        val it = propertyValueIndex
        if (it.isCachedInstance) {
            val minClass = min.javaClass
            val maxClass = max.javaClass
            val cached = it as UpdatablePropertiesCachedInstanceIterable
            return if (minClass != maxClass || minClass != cached.getPropertyValueClass()) {
                EntityIteratorBase.EMPTY
            } else cached.getPropertyRangeIterator(min, max)
        }
        val valueIdx = openCursor(txn) ?: return EntityIteratorBase.EMPTY
        return PropertyRangeIterator(valueIdx)
    }

    override fun getHandleImpl(): EntityIterableHandle {
        val entityTypeId = entityTypeId
        val propertyId = propertyId
        return object : ConstantEntityIterableHandle(store, type) {

            override fun getPropertyIds() = intArrayOf(propertyId)

            override fun toString(builder: StringBuilder) {
                super.toString(builder)
                builder.append(entityTypeId)
                builder.append('-')
                builder.append(propertyId)
                builder.append('-')
                builder.append(min.toString())
                builder.append('-')
                builder.append(max.toString())
            }

            override fun hashCode(hash: EntityIterableHandleHash) {
                hash.apply(entityTypeId)
                hash.applyDelimiter()
                hash.apply(propertyId)
                hash.applyDelimiter()
                hash.apply(min.toString())
                hash.applyDelimiter()
                hash.apply(max.toString())
            }

            override fun getEntityTypeId() = entityTypeId

            override fun isMatchedPropertyChanged(
                id: EntityId,
                propId: Int,
                oldValue: Comparable<*>?,
                newValue: Comparable<*>?
            ): Boolean {
                return propertyId == propId && entityTypeId == id.typeId && (isRangeAffected(oldValue) ||
                        isRangeAffected(newValue))
            }

            private fun isRangeAffected(value: Comparable<*>?): Boolean {
                if (value == null) {
                    return false
                }
                if (value is ComparableSet<*>) {
                    // not null set should be non-empty
                    return isRangeAffectedByPrimitiveValue(value.minimum) ||
                            isRangeAffectedByPrimitiveValue(value.maximum)
                }
                return isRangeAffectedByPrimitiveValue(value)
            }

            private fun isRangeAffectedByPrimitiveValue(value: Comparable<*>): Boolean {
                val lowercaseValue = PropertyTypes.toLowerCase(value)
                return min <= lowercaseValue && max >= lowercaseValue
            }
        }
    }

    override fun countImpl(txn: PersistentStoreTransaction): Long {
        val cursor = openCursor(txn) ?: return 0
        cursor.use {
            val propertyValue = store.propertyTypes.dataToPropertyValue(min)
            val binding = propertyValue.binding
            var result = 0L
            var success = cursor.getSearchKeyRange(propertyValue.dataToEntry()) != null
            while (success && max >= binding.entryToObject(cursor.key)) {
                result += cursor.count()
                success = cursor.nextNoDup
            }
            return result
        }
    }

    private inner class PropertyRangeIterator(cursor: Cursor) : EntityIteratorBase(this@PropertyRangeIterable) {

        private var hasNext = false
        private val binding: ComparableBinding

        init {
            setCursor(cursor)
            binding = store.propertyTypes.dataToPropertyValue(min).binding
            val key: ByteIterable = binding.objectToEntry(min)
            checkHasNext(getCursor().getSearchKeyRange(key) != null)
        }

        public override fun hasNextImpl() = hasNext

        public override fun nextIdImpl(): EntityId? {
            if (hasNextImpl()) {
                explain(type)
                val cursor = cursor
                val result: EntityId = PersistentEntityId(entityTypeId, LongBinding.compressedEntryToLong(cursor.value))
                checkHasNext(cursor.next)
                return result
            }
            return null
        }

        private fun checkHasNext(success: Boolean) {
            hasNext = success && max >= binding.entryToObject(cursor.key)
        }
    }

    companion object {

        init {
            registerType(type) { txn, _, parameters: Array<Any> ->
                try {
                    val min = (parameters[2] as String).toLong()
                    val max = (parameters[3] as String).toLong()
                    return@registerType PropertyRangeIterable(
                        txn,
                        (parameters[0] as String).toInt(),
                        (parameters[1] as String).toInt(),
                        min,
                        max
                    )
                } catch (e: NumberFormatException) {
                    return@registerType PropertyRangeIterable(
                        txn,
                        (parameters[0] as String).toInt(),
                        (parameters[1] as String).toInt(),
                        parameters[2] as Comparable<*>,
                        parameters[3] as Comparable<*>
                    )
                }
            }
        }

        private val type: EntityIterableType get() = EntityIterableType.ENTITIES_BY_PROP_VALUE_IN_RANGE
    }
}
