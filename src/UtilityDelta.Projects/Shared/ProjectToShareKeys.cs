namespace UtilityDelta.Projects.Shared
{
    public class ProjectToShareKeys
    {
        private readonly Dictionary<string, DtoShareKeyData> _shareKeys = [];

        public void AddShareKey(DtoShareKeyData dtoShareKeyData)
        {
            if (_shareKeys.ContainsKey(dtoShareKeyData.hashedCode))
            {
                _shareKeys[dtoShareKeyData.hashedCode] = dtoShareKeyData;
                return;
            }

            _shareKeys.Add(dtoShareKeyData.hashedCode, dtoShareKeyData);
        }

        public bool DisableShareKey(string shareKeyHash)
        {
            if (_shareKeys.ContainsKey(shareKeyHash))
            {
                _shareKeys.Remove(shareKeyHash);
                return true;
            }

            return false;
        }

        public DtoShareKeyData? Find(string shareKeyHash)
        {
            if (_shareKeys.ContainsKey(shareKeyHash))
            {
                return _shareKeys[shareKeyHash];
            }

            return null;
        }

        public bool IsActiveCache { get; set; }

        public int Count => _shareKeys.Count;
    }
}
